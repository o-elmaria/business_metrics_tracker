/* Hard coded values

Store ID = 321, 368, BclgID = 2, 3
Competitors for RPI calculation in the UK --> 'Amazon Buy Box', 'ebayuk', 'argos', 'JohnLewis', 'heatandplumb' (only for plumbing UK)
Competitors for RPI calculation in DE --> 'Amazon Buy Box', 'kaufland', 'otto' (EDIT on 20210702 - changed real to kaufland)
Index dates for WPI and WSI calculation --> All index date greater than or equal to '2019-12-05'

In the final table "class_calc_all_metrics", we use 'Amazon Buy Box' as the chosen competitor and '2019-12-05' as the chosen index date so that the number of rows in all tables match

Backlog:

UKEGM
AtOrderGM values last year
heatandplumb RPI values
*/

-- Step 1: Time horizon this year and last year
-- This step is used to generate all relevant dates for the calculators in combination with the store IDs. The relevant time horizon is defined as follows: the last 3 completed months + current MTD + another 30 days

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT 
  321 AS SoID, -- UK
  now1.*,
  EXTRACT(WEEK FROM Datum_LY) AS WeekNum_LY, -- Extract the WeekNum of the previous year's date
  EXTRACT(DAY FROM Datum_LY) AS Day_LY, -- Extract the day of the previous year's date
  EXTRACT(Month FROM Datum_LY) AS Month_LY -- Extract the month of the previous year's date
FROM (
  SELECT 
    Day_Array AS Datum,
    EXTRACT(WEEK FROM Day_Array) AS WeekNum, -- Extract the WeekNum from Day_Array
    EXTRACT(DAY FROM Day_Array) AS Day, -- Extract the day from Day_Array
    EXTRACT(Month FROM Day_Array) AS Month, -- Extract the month from Day_Array
    DATE_SUB(Day_Array, INTERVAL 365 DAY) AS Datum_LY -- Subtract 365 days from Day_Array to get the previous year's date
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_ADD(CURRENT_DATE, INTERVAL -3 MONTH), MONTH), CURRENT_DATE() + 30, INTERVAL 1 DAY)) AS Day_Array -- Always add 30 days to the current day to cover the **upcoming historical** trends
) now1

UNION ALL 

SELECT 
  368 AS SoID, -- DE
  now2.*,
  EXTRACT(WEEK FROM Datum_LY) AS WeekNum_LY, -- Extract the WeekNum of the previous year's date
  EXTRACT(DAY FROM Datum_LY) AS Day_LY, -- Extract the day of the previous year's date
  EXTRACT(Month FROM Datum_LY) AS Month_LY -- Extract the month of the previous year's date
FROM (
  SELECT 
    Day_Array AS Datum,
    EXTRACT(WEEK FROM Day_Array) AS WeekNum, -- Extract the WeekNum from Day_Array
    EXTRACT(DAY FROM Day_Array) AS Day, -- Extract the day from Day_Array
    EXTRACT(Month FROM Day_Array) AS Month, -- Extract the month from Day_Array
    DATE_SUB(Day_Array, INTERVAL 365 DAY) AS Datum_LY -- Subtract 365 days from Day_Array to get the previous year's date
  FROM UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE_ADD(CURRENT_DATE, INTERVAL -3 MONTH), MONTH), CURRENT_DATE() + 30, INTERVAL 1 DAY)) AS Day_Array -- Always add 30 days to the current day to cover the **upcoming historical** trends
) now2

ORDER BY SoID, Datum;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2: Class calculator tables

-- Step 2.1: Non-spreadbased metrics (GRS, total order count, total ordered products, OrderShare, Cart Quantity, AOV, ACO, and AUP)

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_non_sb_metrics`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_non_sb_metrics`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- GRS, total order count, total ordered products, OrderShare, Cart Quantity, AOV, ACO, and AUP this year
  ROUND(now.GRS, 2) AS GRS,
  now.TotalOrderCount,
  now.TotalOrderedProducts,
  ROUND(now.OrderShare, 2) AS OrderShare,
  ROUND(now.CartQuantity, 2) AS CartQuantity,
  ROUND(now.GRS / NULLIF(now.TotalOrderCount, 0), 2) AS Average_Contribution_to_Order,
  ROUND(now.GRS / NULLIF(now.OrderShare,0), 2) AS Average_Order_Value,
  ROUND(now.GRS / NULLIF(now.CartQuantity,0), 2) AS Average_Unit_Price,

  -- GRS, total order count, total ordered products, OrderShare, Cart Quantity, AOV, ACO, and AUP over the same time horizon but last year
  ROUND(ly.GRS_LY, 2) AS GRS_LY,
  ly.TotalOrderCount_LY,
  ly.TotalOrderedProducts_LY,
  ROUND(ly.OrderShare_LY, 2) AS OrderShare_LY,
  ROUND(ly.CartQuantity_LY, 2) AS CartQuantity_LY,
  ROUND(ly.GRS_LY / NULLIF(ly.TotalOrderCount_LY,0), 2) AS Average_Contribution_to_Order_LY,
  ROUND(ly.GRS_LY / NULLIF(ly.OrderShare_LY,0), 2) AS Average_Order_Value_LY,
  ROUND(ly.GRS_LY / NULLIF(ly.CartQuantity_LY,0), 2) AS Average_Unit_Price_LY
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID
LEFT JOIN ( -- This sub-query calculates the daily GRS, total order count, total ordered products, OrderShare, and Cart Quantity over the last 3 completed months + current MTD + another 30 days
  SELECT 
    rt.orderdate, 
    rt.soid AS SoID,
    o.MkcID,
    o.ClID,
    SUM(grossrevenuestable) AS GRS,
    COUNT(DISTINCT rt.orid) AS TotalOrderCount,
    COUNT(DISTINCT rt.opid) AS TotalOrderedProducts,
    SUM(rt.ordershare) AS OrderShare,
    SUM(rt.cartqty) AS CartQuantity
  FROM `wf-gcp-us-ae-retail-prod.cm_reporting.retail_fact_order_product_revenue_cost` rt -- Table containing the commercial metrics
  INNER JOIN `wf-gcp-us-ae-retail-prod.cm_reporting.retail_dim_order` AS retailor ON retailor.orderkey = rt.orderkey -- Table containing the field that is used to identify cancelled and uncancelled orders
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order_product` o ON o.opid = rt.opid -- For excluding "External Order Source" in the WHERE clause
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order` d ON d.orid = o.oporid -- For excluding "External Order Source" in the WHERE clause
  WHERE 1=1
    AND CAST(rt.orderdate AS DATE) IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates this year
    AND rt.soid IN (321,368) -- 321 for UK, 368 for DE
    AND businesssector = 0 -- Direct online
    AND retailor.orcancelled != 1 -- Exclude cancellations
    AND d.orosid != 17 -- Exclude External Order Source
  GROUP BY 1,2,3,4
) now ON day.Datum = now.orderdate AND day.SoID = now.SoID AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID 

LEFT JOIN ( -- This sub-query calculates the daily GRS, total order count, total ordered products, OrderShare, and Cart Quantity over the same time period as above, BUT LAST YEAR
  SELECT 
    rt.orderdate AS orderdate_LY, 
    rt.soid AS SoID,
    o.MkcID,
    o.ClID,
    SUM(grossrevenuestable) AS GRS_LY,
    COUNT(DISTINCT rt.orid) AS TotalOrderCount_LY,
    COUNT(DISTINCT rt.opid) AS TotalOrderedProducts_LY,
    SUM(rt.ordershare) AS OrderShare_LY,
    SUM(rt.cartqty) AS CartQuantity_LY
  FROM `wf-gcp-us-ae-retail-prod.cm_reporting.retail_fact_order_product_revenue_cost` rt -- Table containing the commercial metrics
  INNER JOIN `wf-gcp-us-ae-retail-prod.cm_reporting.retail_dim_order` AS retailor ON retailor.orderkey = rt.orderkey -- Table containing the field that is used to identify cancelled and uncancelled orders
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order_product` o ON o.opid = rt.opid -- For excluding "External Order Source" in the WHERE clause
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order` d ON d.orid = o.oporid -- For excluding "External Order Source" in the WHERE clause
  WHERE 1=1
    -- Cover the same timeframe as above, BUT LAST YEAR
    AND CAST(rt.orderdate AS DATE) IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates last year
    AND rt.soid IN (321,368) -- 321 for UK, 368 for DE
    AND businesssector = 0 -- Direct online
    AND retailor.orcancelled != 1 -- Exclude cancellations
    AND d.orosid != 17 -- Exclude External Order Source
  GROUP BY 1,2,3,4
) ly ON day.Datum_LY = ly.orderdate_LY AND day.SoID = ly.SoID AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.2: Spreadbased Metrics (Spread-based GM, Spread-based VCD, Spread-based net revenue, Spread-based gross profit)

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_sb_metrics`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_sb_metrics`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- Spread-based GM, Spread-based VCD, Spread-based net revenue, and Spread-based gross profit of this year
  ROUND(now.Spreadbased_NetRevenue, 2) AS Spreadbased_NetRevenue,
  ROUND(now.Spreadbased_GrossProfit, 2) AS Spreadbased_GrossProfit,
  ROUND(now.SpreadBased_GM, 4) AS SpreadBased_GM,
  ROUND(now.SpreadBased_GM_Excl_Fulfillment, 4) AS SpreadBased_GM_Excl_Fulfillment,
  ROUND(now.Spreadbased_VCD, 2) AS Spreadbased_VCD,
  ROUND(now.SpreadBased_VCM, 4) AS SpreadBased_VCM,

  -- Spread-based GM, Spread-based VCD, Spread-based net revenue, and Spread-based gross profit over the same time horizon but last year
  ROUND(ly.Spreadbased_NetRevenue_LY, 2) AS Spreadbased_NetRevenue_LY,
  ROUND(ly.Spreadbased_GrossProfit_LY, 2) AS Spreadbased_GrossProfit_LY,
  ROUND(ly.SpreadBased_GM_LY, 4) AS SpreadBased_GM_LY,
  ROUND(ly.SpreadBased_GM_Excl_Fulfillment_LY, 4) AS SpreadBased_GM_Excl_Fulfillment_LY,
  ROUND(ly.Spreadbased_VCD_LY, 2) AS Spreadbased_VCD_LY,
  ROUND(ly.SpreadBased_VCM_LY, 4) AS SpreadBased_VCM_LY
  
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID
LEFT JOIN ( -- This sub-query calculates the daily Spread-based GM, Spread-based VCD, Spread-based net revenue, and Spread-based gross profit over the last 3 completed months + current MTD + another 30 days
  SELECT 
    rt.orderdate, 
    rt.soid AS SoID,
    o.MkcID,
    o.ClID,
    SUM(spreadnetrevenue) AS Spreadbased_NetRevenue,
    SUM(spreadgrossprofit) AS Spreadbased_GrossProfit,
    SUM(spreadgrossprofit) / NULLIF(SUM(spreadnetrevenue), 0) AS SpreadBased_GM,
    SUM(spreadgrossprofitprefulfillment) / NULLIF(SUM(spreadnetrevenueprefulfillment), 0) AS SpreadBased_GM_Excl_Fulfillment,
    SUM(spreadcontribution) AS Spreadbased_VCD,
    SUM(spreadcontribution) / NULLIF(SUM(spreadnetrevenue), 0) AS Spreadbased_VCM,
  FROM `wf-gcp-us-ae-retail-prod.cm_reporting.retail_fact_order_product_revenue_cost` rt -- Table containing the commercial metrics
  INNER JOIN `wf-gcp-us-ae-retail-prod.cm_reporting.retail_dim_order` AS retailor ON retailor.orderkey = rt.orderkey -- Table containing the field that is used to identify cancelled and uncancelled orders
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order_product` o ON o.opid = rt.opid -- For excluding "External Order Source" in the WHERE clause
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order` d ON d.orid = o.oporid -- For excluding "External Order Source" in the WHERE clause
  WHERE 1=1
    AND CAST(rt.orderdate AS DATE) IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates this year
    AND rt.soid IN (321,368) -- 321 for UK, 368 for DE
    AND primarypymtid NOT IN (31, 10, 5, 18, 4, 53, -999, 0, 3) -- Exclude the following payment methods ("ACH/Wire", "Bad Debt Writeoff", "Check", "Vorkasse", "Purchase Order", "Liquidation", "Unknown", "Unknown ID 0", and "Other")
    AND businesssector = 0 -- Direct online
    AND retailor.orcancelled != 1 -- Exclude cancellations
    AND d.orosid != 17 -- Exclude External Order Source
  GROUP BY 1,2,3,4
) now ON day.Datum = now.orderdate AND day.SoID = now.SoID AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID

LEFT JOIN ( -- This sub-query calculates the daily Spread-based GM, Spread-based VCD, Spread-based net revenue, and Spread-based gross profit over the same time period as above, BUT LAST YEAR
  SELECT 
    rt.orderdate AS orderdate_LY, 
    rt.soid AS SoID,
    o.MkcID,
    o.ClID,
    SUM(spreadnetrevenue) AS Spreadbased_NetRevenue_LY,
    SUM(spreadgrossprofit) AS Spreadbased_GrossProfit_LY,
    SUM(spreadgrossprofit) / NULLIF(SUM(spreadnetrevenue), 0) AS SpreadBased_GM_LY,
    SUM(spreadgrossprofitprefulfillment) / NULLIF(SUM(spreadnetrevenueprefulfillment), 0) AS SpreadBased_GM_Excl_Fulfillment_LY,
    SUM(spreadcontribution) AS Spreadbased_VCD_LY,
    SUM(spreadcontribution) / NULLIF(SUM(spreadnetrevenue), 0) AS Spreadbased_VCM_LY
  FROM `wf-gcp-us-ae-retail-prod.cm_reporting.retail_fact_order_product_revenue_cost` rt -- Table containing the commercial metrics
  INNER JOIN `wf-gcp-us-ae-retail-prod.cm_reporting.retail_dim_order` AS retailor ON retailor.orderkey = rt.orderkey -- Table containing the field that is used to identify cancelled and uncancelled orders
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order_product` o ON o.opid = rt.opid -- For excluding "External Order Source" in the WHERE clause
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order` d ON d.orid = o.oporid -- For excluding "External Order Source" in the WHERE clause
  WHERE 1=1
    -- Cover the same timeframe as above, BUT LAST YEAR
    AND CAST(rt.orderdate AS DATE) IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates last year
    AND rt.soid IN (321,368) -- 321 for UK, 368 for DE
    AND primarypymtid NOT IN (31, 10, 5, 18, 4, 53, -999, 0, 3) -- Exclude the following payment methods ("ACH/Wire", "Bad Debt Writeoff", "Check", "Vorkasse", "Purchase Order", "Liquidation", "Unknown", "Unknown ID 0", and "Other")
    AND businesssector = 0 -- Direct online
    AND retailor.orcancelled != 1 -- Exclude cancellations
    AND d.orosid != 17 -- Exclude External Order Source
  GROUP BY 1,2,3,4
) ly ON day.Datum_LY = ly.orderdate_LY AND day.SoID = ly.SoID AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.3: PPM and AtOrderGM

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ppm_atordergm`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ppm_atordergm`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- PPM and AtOrder GM this year
  ROUND(now.PPM, 4) AS PPM,
  ROUND(now.AtOrderGM, 4) AS AtOrderGM,

  -- PPM and AtOrder GM over the same time horizon but last year
  ROUND(ly.PPM_LY, 4) AS PPM_LY,
  ROUND(ly.AtOrderGM_LY, 2) AS AtOrderGM_LY
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID
LEFT JOIN ( -- This sub-query calculates the daily PPM and AtOrderGM over the last 3 completed months + current MTD + another 30 days
  SELECT 
    DATE(orca.orcompletedate) AS Datum,
    orca.soid AS SoID,
    o.MkcID,
    o.ClID,
    (1 - SUM(orca.productcostdtp)/SUM(orca.productrevenuedtp)) AS PPM,
    (100 * (1 - SUM(orca.originalopqty * (opid.OpcTotalCost - opid.OpcPricingWholesaleCost) + orca.productcostdtp)/SUM((orca.productrevenuedtp * opid.OpcRevenueAdjustmentRatio) + (opid.OpcShippingCustomerRevenue/(1 + opid.opcproductrevenuetaxratio)))) - 0.5) / 100 AS AtOrderGM
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_part_original_opid_revenue_cost_actuals` orca
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order_product` o ON o.opid = orca.opid -- For linking to Mkcname and MkcID
  LEFT JOIN `wf-gcp-us-ae-bulk-prod.bulk_csn_pricing.tbl_opid_pricing_inputs_snapshot_b2c` opid -- Important to have a LEFT JOIN here because some dates in "tbl_opid_pricing_inputs_snapshot_b2c" could be missing
      ON orca.manufacturerpartid = opid.OpcManufacturerPartId
      AND orca.opid = opid.OpcOpId
      AND orca.bclgid = opid.opcbclgid
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order` d ON d.orid = orca.orid -- (EDIT on 20210702 - added this INNER JOIN to remove liquidation orders)
  WHERE 1=1
      AND orca.soid IN (321,368) -- 321 for UK, 368 for DE
      AND DATE(orca.orcompletedate) IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates this year
      AND orca.TotalLiquidationDiscountRevenuePreTax = 0 -- Removes liquidation orders (liquidations are one-off warehouse events and will lead to extreme adjustments; also unfair to distribute across short 3-month timeframe)
      AND orca.ProductCostDTP > 0 -- Ignore orders with $0 original costs (zero-cost tile/fabric samples and other odd items that should not impact pricing)
      AND orca.ProductRevenueDTP > 0 
      AND orca.CancelledOriginalOpQty = 0 -- Excludes cancelled orders (cancellations do not incur cost for Wayfair)
      AND opid.OpcTotalCost > 0
	  AND d.OrOsID <> 17 -- Removes liquidation orders (EDIT on 20210702 - added this condition to remove liquidation orders)
      -- Additional standard Orca filters
      AND orca.isGiftCardPurchase = 0 -- Removes gift cards (gift cards tend to overstate revenue and margin)
      AND orca.SalePriceOnSitePerQtyPreTax > 0 -- Ignore free stuff to avoid wrong TOC (Some products are shipped as multiple OpIDs, some of which are free in some cases, e.g. you buy a bed and it comes with a free bolt-set, but we also sell the bolt set alone)
      AND orca.OriginalResoldOpQty = 0 -- Excludes open box / resold products
      AND ABS(orca.ProductCostDTP / orca.OriginalOpQty - 9999) >= .005 -- Ignore default value cases where ProductCostDTP is a multiple of 9999
  GROUP BY 1,2,3,4
) now ON day.Datum = now.Datum AND day.SoID = now.SoID AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID 

LEFT JOIN ( -- This sub-query calculates the daily PPM and AtOrderGM over the same time period as above, BUT LAST YEAR
  SELECT 
    DATE(orca.orcompletedate) AS Datum_LY,
    orca.soid AS SoID,
    o.MkcID,
    o.ClID,
    (1 - SUM(orca.productcostdtp)/SUM(orca.productrevenuedtp)) AS PPM_LY,
    (100 * (1 - SUM(orca.originalopqty * (opid.OpcTotalCost - opid.OpcPricingWholesaleCost) + orca.productcostdtp)/SUM((orca.productrevenuedtp * opid.OpcRevenueAdjustmentRatio) + (opid.OpcShippingCustomerRevenue/(1 + opid.opcproductrevenuetaxratio)))) - 0.5) / 100 AS AtOrderGM_LY
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_part_original_opid_revenue_cost_actuals` orca
  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order_product` o ON o.opid = orca.opid -- For linking to Mkcname and MkcID
  LEFT JOIN `wf-gcp-us-ae-bulk-prod.bulk_csn_pricing.tbl_opid_pricing_inputs_snapshot_b2c` opid -- Important to have a LEFT JOIN here because some dates in "tbl_opid_pricing_inputs_snapshot_b2c" could be missing
      ON orca.manufacturerpartid = opid.OpcManufacturerPartId
      AND orca.opid = opid.OpcOpId
      AND orca.bclgid = opid.opcbclgid
	  INNER JOIN `wf-gcp-us-ae-sql-data-prod.elt_order.tbl_order` d ON d.orid = orca.orid -- (EDIT on 20210702 - added this INNER JOIN to remove liquidation orders)
  WHERE 1=1
      AND orca.soid IN (321,368) -- 321 for UK, 368 for DE
      AND DATE(orca.orcompletedate) IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates this year
      AND orca.TotalLiquidationDiscountRevenuePreTax = 0 -- Removes liquidation orders (liquidations are one-off warehouse events and will lead to extreme adjustments; also unfair to distribute across short 3-month timeframe)
      AND orca.ProductCostDTP > 0 -- Ignore orders with $0 original costs (zero-cost tile/fabric samples and other odd items that should not impact pricing)
      AND orca.ProductRevenueDTP > 0 
      AND orca.CancelledOriginalOpQty = 0 -- Excludes cancelled orders (cancellations do not incur cost for Wayfair)
	  AND d.OrOsID <> 17 -- Removes liquidation orders (EDIT on 20210702 - added this condition to remove liquidation orders)
      --AND opid.OpcTotalCost > 0 -- Remove this filter as "tbl_opid_pricing_inputs_snapshot_b2c" has missing dates in the past, so adding a condition referring to this table will truncate the results
      -- Additional standard filters
      AND orca.isGiftCardPurchase = 0 -- Removes gift cards (gift cards tend to overstate revenue and margin)
      AND orca.SalePriceOnSitePerQtyPreTax > 0 -- Ignore free stuff to avoid wrong TOC (Some products are shipped as multiple OpIDs, some of which are free in some cases, e.g. you buy a bed and it comes with a free bolt-set, but we also sell the bolt set alone)
      AND orca.OriginalResoldOpQty = 0 -- Excludes open box / resold products
      AND ABS(orca.ProductCostDTP / orca.OriginalOpQty - 9999) >= .005 -- Ignore default value cases where ProductCostDTP is a multiple of 9999
  GROUP BY 1,2,3,4
) ly ON day.Datum_LY = ly.Datum_LY AND day.SoID = ly.SoID AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.4: Visits

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_visits`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_visits`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,
  
  -- Distinct visits and page views of this year
  now.Visits,
  now.PageViews, 

  -- Distinct visits and page views of last year
  ly.Visits_LY,
  ly.PageViews_LY
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID
LEFT JOIN ( -- This sub-query calculates the daily visits and page views over the last 3 completed months + current MTD + another 30 days
  SELECT
    v.SessionStartDate,
    v.Event_SoID,
    cls.ClMkcID AS MkcID,
    v.ClID,
    COUNT(DISTINCT v.Event_SessionKey) AS Visits,
    SUM(1) AS PageViews
  FROM `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits_class_views` v  -- EDIT on 20210702 - to include class level visits in order to avoid joining tbl_dash_clicks_product_request table  
  -- (EDIT on 20210702 - commented out the INNER JOIN to pull the entire traffic coming to the website)
  -- INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_clicks_product_request` pr
  -- ON  pr.SessionStartDate = v.SessionStartDate
  --  AND pr.Event_SoID = v.Event_SoID
  --  AND pr.Event_SessionKey = v.Event_SessionKey
  INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits_exclusion_reason` ex 
    ON  ex.SessionStartDate = v.SessionStartDate
    AND ex.Event_SoID = v.Event_SoID
    AND ex.Event_SessionKey = v.Event_SessionKey
  -- INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_join_product_class` jpc ON pr.Event_PrSKU = jpc.PrSKU AND PcMasterClass = TRUE -- Join SKU to class
   INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON v.ClID = cls.ClID -- Join class to category
  WHERE 1=1
    AND v.Event_SoID IN (321,368) -- 321 for UK, 368 for DE
    AND v.SessionStartDate IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates this year
    AND ex.VisitExclusionreason = 0 -- That's a standard filter to get the right number of visits and views
     -- (EDIT on 20210702 - commented out the condition associated with the table aliased by "pr")
    --AND pr.Event_PrSKU IS NOT NULL -- Eliminate sessions where the SKU visited was not on record (Uncomment if you want to pull the visits that have at least 1 SKU view. Commenting this part out pulls the total traffic coming to the website)
  GROUP BY 1,2,3,4 
) now ON day.Datum = now.SessionStartDate AND day.SoID = now.Event_SoID AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID

LEFT JOIN ( -- This sub-query calculates the daily visits and page views over the same time period as above, BUT LAST YEAR
  SELECT
    v.SessionStartDate AS SessionStartDate_LY,
    v.Event_SoID,
    cls.ClMkcID AS MkcID,
    v.ClID,
    COUNT(DISTINCT v.Event_SessionKey) AS Visits_LY,
    SUM(1) AS PageViews_LY
  FROM `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits_class_views` v  -- EDIT on 20210702 - to include class level visits in order to avoid joining tbl_dash_clicks_product_request table  
   -- (EDIT on 20210702 - commented out the INNER JOIN to pull the entire traffic coming to the website)
  -- INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_clicks_product_request` pr
  --  ON  pr.SessionStartDate = v.SessionStartDate
  --  AND pr.Event_SoID = v.Event_SoID
  --  AND pr.Event_SessionKey = v.Event_SessionKey
  INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits_exclusion_reason` ex 
    ON  ex.SessionStartDate = v.SessionStartDate
    AND ex.Event_SoID = v.Event_SoID
    AND ex.Event_SessionKey = v.Event_SessionKey
  --INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_join_product_class` jpc ON pr.Event_PrSKU = jpc.PrSKU AND PcMasterClass = TRUE -- Join SKU to class
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON v.ClID = cls.ClID -- Join class to category
  WHERE 1=1
    AND v.Event_SoID IN (321,368) -- 321 for UK, 368 for DE
    AND v.SessionStartDate IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`) -- The relevant list of dates last year
    AND ex.VisitExclusionreason = 0 -- That's a standard filter to get the right number of visits and views
    -- (EDIT on 20210702 - commented out the condition associated with the table aliased by "pr")
    --AND pr.Event_PrSKU IS NOT NULL -- Eliminate sessions where the SKU visited was not on record (-- Uncomment if you want to pull the visits that have at least 1 SKU view. Commenting this part out pulls the total traffic coming to the website)
  GROUP BY 1,2,3,4
) ly ON day.Datum_LY = ly.SessionStartDate_LY AND day.SoID = ly.Event_SoID AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.5: Revenue per visit (RPV), gross profit per visit (PPV), and conversion rate (CR)

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ppv_rpv_cr`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ppv_rpv_cr`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT
  -- Grouping variables
  sb.SoID,
  sb.Datum,
  sb.WeekNum,
  sb.Day,
  sb.Month,
  sb.Datum_LY,
  sb.MkcName,
  sb.MkcID,
  sb.ClName,
  sb.ClID,

  -- Metrics
  ROUND(GRS / Visits, 4) AS RPV,
  ROUND(GRS_LY / Visits_LY, 4) AS RPV_LY,

  ROUND(Spreadbased_GrossProfit / Visits, 4) AS PPV,
  ROUND(Spreadbased_GrossProfit_LY / Visits_LY, 4) AS PPV_LY,

  ROUND(TotalOrderCount / Visits, 4) AS CR,
  ROUND(TotalOrderCount_LY / Visits_LY, 4) AS CR_LY
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_non_sb_metrics` sb
INNER JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_sb_metrics` nonsb ON sb.SoID = nonsb.SoID AND sb.Datum = nonsb.Datum AND sb.Datum_LY = nonsb.Datum_LY AND sb.MkcID = nonsb.MkcID AND sb.ClID = nonsb.ClID
INNER JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_visits` vis ON sb.SoID = vis.SoID AND sb.Datum = vis.Datum AND sb.Datum_LY = vis.Datum_LY AND sb.MkcID = vis.MkcID AND sb.ClID = vis.ClID;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.6: Indices (RPI, WPI, WSI)

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_rpi`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_rpi`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

-- RPI
SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  comp.CompetitorName,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- RPI this year
  ROUND(RPI0, 4) AS RPI0,
  ROUND(RPI28, 4) AS RPI28,

  -- RPI last year
  ROUND(RPI0_LY, 4) AS RPI0_LY,
  ROUND(RPI28_LY, 4) AS RPI28_LY
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day
-- (EDIT on 20210702 - replaced real with kaufland and added home24 and reuter to the DE competitor list)
CROSS JOIN (
 SELECT 'Amazon Buy Box' AS CompetitorName UNION ALL SELECT 'ebayuk' AS CompetitorName UNION ALL SELECT 'argos' AS CompetitorName UNION ALL SELECT 'JohnLewis' AS CompetitorName UNION ALL SELECT 'heatandplumb' AS CompetitorName
  UNION ALL SELECT 'kaufland' AS CompetitorName UNION ALL SELECT 'otto' AS CompetitorName UNION ALL SELECT 'home24' AS CompetitorName UNION ALL SELECT 'reuter' AS CompetitorName
) comp -- Cross join ALL competitor names to every SoID-Datum-WeekNum-Day-Month-Datum_LY row. This essentially creates a 7-row partition out of every row. We get rid of irrelevant competitors to the geo down below
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID
LEFT JOIN ( -- This sub-query calculates the daily RPI over the last 3 completed months + current MTD + another 30 days
  SELECT 
    rpi.insertdate AS Datum, 
    rpi.soid AS SoID,
    cls.ClMkcID AS MkcID,
    rpi.ClID,
    com.competitorname AS CompetitorName,
    SUM(RPI0D_Numerator)/SUM(RPI0D_Denominator) AS RPI0,
    SUM(RPI28D_Numerator)/SUM(RPI28D_Denominator) AS RPI28
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_rpi_sku` rpi
  INNER JOIN `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_dim_pc_opc_competitor` com ON com.competitorid = rpi.competitorID
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON rpi.ClID = cls.ClID -- Join class to category 
  WHERE 1=1 -- (EDIT on 20210702 - replaced real with kaufland and added home24 and reuter to the DE competitor list)
    AND rpi.insertdate IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND CompetitorName IN ('Amazon Buy Box', 'ebayuk', 'argos', 'JohnLewis', 'kaufland', 'otto', 'heatandplumb', 'home24', 'reuter') -- Tier one competitors in the UK and DE (heatandplumb is only relevant for UK plumbing)
    AND rpi.soid IN (321,368)
  GROUP BY 1,2,3,4,5
) now ON day.Datum = now.Datum AND day.SoID = now.SoID AND comp.CompetitorName = now.CompetitorName AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID

LEFT JOIN ( -- This sub-query calculates the daily RPI over the same time period as above, BUT LAST YEAR
  SELECT 
    rpi.insertdate AS Datum_LY, 
    rpi.soid AS SoID,
    cls.ClMkcID AS MkcID,
    rpi.ClID,
    com.competitorname AS CompetitorName,
    SUM(RPI0D_Numerator)/SUM(RPI0D_Denominator) AS RPI0_LY,
    SUM(RPI28D_Numerator)/SUM(RPI28D_Denominator) AS RPI28_LY
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_rpi_sku` rpi
  INNER JOIN `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_dim_pc_opc_competitor` com ON com.competitorid = rpi.competitorID
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON rpi.ClID = cls.ClID -- Join class to category
  WHERE 1=1 -- (EDIT on 20210702 - replaced real with kaufland and added home24 and reuter to the DE competitor list)
    AND rpi.insertdate IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND CompetitorName IN ('Amazon Buy Box', 'ebayuk', 'argos', 'JohnLewis', 'kaufland', 'otto', 'heatandplumb', 'home24', 'reuter') -- Tier one competitors in the UK and DE (heatandplumb is only relevant for UK plumbing)
    AND rpi.soid IN (321,368)
  GROUP BY 1,2,3,4,5
) ly ON day.Datum_LY = ly.Datum_LY AND day.SoID = ly.SoID AND comp.CompetitorName = ly.CompetitorName AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
-- (EDIT on 20210702 - replaced real with kaufland and added home24 and reuter to the DE competitor list)
WHERE (day.SoID = 321 AND comp.CompetitorName IN ('Amazon Buy Box', 'ebayuk', 'argos', 'JohnLewis', 'heatandplumb')) OR (day.SoID = 368 AND comp.CompetitorName IN ('Amazon Buy Box', 'otto', 'kaufland', 'home24', 'reuter')) -- Eliminating irrelevant competitors to the store
ORDER BY 1, cl.Mkcname, cl.ClName, 2,7;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_wpi`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_wpi`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

-- WPI
SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  ind.indexdate AS IndexDate,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- WPI this year
  ROUND(WPI0, 4) AS WPI0,
  ROUND(WPI28, 4) AS WPI28,

  -- WPI last year
  ROUND(WPI0_LY, 4) AS WPI0_LY,
  ROUND(WPI28_LY, 4) AS WPI28_LY

FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day
-- Cross join ALL index dates >= '2019-12-05' to every SoID-Datum-WeekNum-Day-Month-Datum_LY row. This essentially creates a n-row partition out of every row where n is the number of index dates in the SELECT statement below
CROSS JOIN (SELECT DISTINCT indexdate FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wpi_sku` WHERE indexdate >= DATE('2019-12-05')) ind
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID
LEFT JOIN ( -- This sub-query calculates the daily WPI over the last 3 completed months + current MTD + another 30 days
  SELECT 
    wpi.insertdate AS Datum,
    wpi.indexdate AS IndexDate,
    wpi.soid AS SoID,
    cls.ClMkcID AS MkcID,
    cls.ClID,
    SUM(WPI0D_Numerator)/SUM(WPI0D_Denominator) AS WPI0,
    SUM(WPI28D_Numerator)/SUM(WPI28D_Denominator) AS WPI28
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wpi_sku` wpi
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON wpi.ClID = cls.ClID -- Join class to category 
  WHERE 1=1
    AND wpi.insertdate IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND wpi.indexdate IN (SELECT DISTINCT indexdate FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wpi_sku` WHERE indexdate >= DATE('2019-12-05')) -- All index dates >= '2019-12-05'
    AND wpi.soid IN (321,368)
  GROUP BY 1,2,3,4,5
) now ON day.Datum = now.Datum AND day.SoID = now.SoID AND ind.indexdate = now.IndexDate AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID 

LEFT JOIN ( -- This sub-query calculates the daily WPI over the same time period as above, BUT LAST YEAR
  SELECT 
    wpi.insertdate AS Datum_LY,
    wpi.indexdate AS IndexDate_LY,
    wpi.soid AS SoID,
    cls.ClMkcID AS MkcID,
    cls.ClID AS ClID,
    SUM(WPI0D_Numerator)/SUM(WPI0D_Denominator) AS WPI0_LY,
    SUM(WPI28D_Numerator)/SUM(WPI28D_Denominator) AS WPI28_LY
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wpi_sku` wpi
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON wpi.ClID = cls.ClID -- Join class to category 
  WHERE 1=1
    AND wpi.insertdate IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND wpi.indexdate IN (SELECT DISTINCT indexdate FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wpi_sku` WHERE indexdate >= DATE('2019-12-05')) -- All index dates >= '2019-12-05'
    AND wpi.soid IN (321,368)
  GROUP BY 1,2,3,4,5
) ly ON day.Datum_LY = ly.Datum_LY AND day.SoID = ly.SoID AND ind.indexdate = ly.IndexDate_LY AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2,7;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_wsi`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_wsi`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

-- WSI
SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  ind.indexdate AS IndexDate,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- WSI this year
  ROUND(WSI0, 4) AS WSI0,
  ROUND(WSI28, 4) AS WSI28,

  -- WSI last year
  ROUND(WSI0_LY, 4) AS WSI0_LY,
  ROUND(WSI28_LY, 4) AS WSI28_LY

FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day
-- Cross join ALL index dates >= '2019-12-05' to every SoID-Datum-WeekNum-Day-Month-Datum_LY row. This essentially creates a n-row partition out of every row where n is the number of index dates in the SELECT statement below
CROSS JOIN (SELECT DISTINCT indexdate FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wsi_sku` WHERE indexdate >= DATE('2019-12-05')) ind
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID
LEFT JOIN ( -- This sub-query calculates the daily WSI over the last 3 completed months + current MTD + another 30 days
  SELECT 
    wsi.insertdate AS Datum,
    wsi.indexdate AS IndexDate,
    wsi.soid AS SoID,
    cls.ClMkcID AS MkcID,
    cls.ClID,
    SUM(WSI0D_Numerator)/SUM(WSI0D_Denominator) AS WSI0,
    SUM(WSI28D_Numerator)/SUM(WSI28D_Denominator) AS WSI28
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wsi_sku` wsi
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON wsi.ClID = cls.ClID -- Join class to category 
  WHERE 1=1
    AND wsi.insertdate IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND wsi.indexdate IN (SELECT DISTINCT indexdate FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wsi_sku` WHERE indexdate >= DATE('2019-12-05')) -- All index dates >= '2019-12-05'
    AND wsi.soid IN (321,368)
  GROUP BY 1,2,3,4,5
) now ON day.Datum = now.Datum AND day.SoID = now.SoID AND ind.indexdate = now.IndexDate AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID 

LEFT JOIN ( -- This sub-query calculates the daily WSI over the same time period as above, BUT LAST YEAR
  SELECT 
    wsi.insertdate AS Datum_LY,
    wsi.indexdate AS IndexDate_LY,
    wsi.soid AS SoID,
    cls.ClMkcID AS MkcID,
    cls.ClID AS ClID,
    SUM(WSI0D_Numerator)/SUM(WSI0D_Denominator) AS WSI0_LY,
    SUM(WSI28D_Numerator)/SUM(WSI28D_Denominator) AS WSI28_LY
  FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wsi_sku` wsi
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON wsi.ClID = cls.ClID -- Join class to category 
  WHERE 1=1
    AND wsi.insertdate IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND wsi.indexdate IN (SELECT DISTINCT indexdate FROM `wf-gcp-us-ae-pricing-prod.pricing_dw.tbl_fact_pc_opc_wsi_sku` WHERE indexdate >= DATE('2019-12-05')) -- All index dates >= '2019-12-05'
    AND wsi.soid IN (321,368)
  GROUP BY 1,2,3,4,5
) ly ON day.Datum_LY = ly.Datum_LY AND day.SoID = ly.SoID AND ind.indexdate = ly.IndexDate_LY AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2,7;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.7: Availability

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_availability`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_availability`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT 
  -- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- Availability, Findability and In-Stock metrics this year (some metrics are commented out because they are zeroed out in the "tbl_agg_retail_availability" table)

  -- Availability
  ROUND(Demand_Weighted_Availability, 4) AS Demand_Weighted_Availability,
  --ROUND(Demand_Weighted_Availability_CG, 4) AS Demand_Weighted_Availability_CG, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_Availability_Stocking, 4) AS Demand_Weighted_Availability_Stocking, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_Availability_Dropship, 4) AS Demand_Weighted_Availability_Dropship,
  --ROUND(Demand_Weighted_Availability - Demand_Weighted_Availability_Dropship, 4) AS Demand_Weighted_Availability_CGAndStocking,
  ROUND(Unweighted_Availability, 4) AS Unweighted_Availability,

  -- Findability
  ROUND(Demand_Weighted_Findability, 4) AS Demand_Weighted_Findability,
  --ROUND(Demand_Weighted_Findability_CG, 4) AS Demand_Weighted_Findability_CG,
  --ROUND(Demand_Weighted_Findability_Stocking, 4) AS Demand_Weighted_Findability_Stocking,
  --ROUND(Demand_Weighted_Findability_Dropship, 4) AS Demand_Weighted_Findability_Dropship,
  --ROUND(Demand_Weighted_Findability - Demand_Weighted_Findability_Dropship, 4) AS Demand_Weighted_Findability_CGAndStocking,
  ROUND(Unweighted_Findability, 4) AS Unweighted_Findability,

  -- In-Stock Rate
  ROUND(Demand_Weighted_InStockRate, 4) AS Demand_Weighted_InStockRate,
  --ROUND(Demand_Weighted_InStockRate_CG, 4) AS Demand_Weighted_InStockRate_CG, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_InStockRate_Stocking, 4) AS Demand_Weighted_InStockRate_Stocking, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_InStockRate_Dropship, 4) AS Demand_Weighted_InStockRate_Dropship,
  --ROUND(Demand_Weighted_InStockRate - Demand_Weighted_InStockRate_Dropship, 4) AS Demand_Weighted_InStockRate_CGAndStocking,
  ROUND(Unweighted_InStockRate, 4) AS Unweighted_InStockRate,

  -- Availability, Findability and In-Stock metrics over the same time period as above BUT LAST YEAR (some metrics are commented out because they are zeroed out in the "tbl_agg_retail_availability" table)

  -- Availability Last Year
  ROUND(Demand_Weighted_Availability_LY, 4) AS Demand_Weighted_Availability_LY,
  --ROUND(Demand_Weighted_Availability_CG_LY, 4) AS Demand_Weighted_Availability_CG_LY, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_Availability_Stocking_LY, 4) AS Demand_Weighted_Availability_Stocking_LY, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_Availability_Dropship_LY, 4) AS Demand_Weighted_Availability_Dropship_LY,
  --ROUND(Demand_Weighted_Availability_LY - Demand_Weighted_Availability_Dropship_LY, 4) AS Demand_Weighted_Availability_CGAndStocking_LY,
  ROUND(Unweighted_Availability_LY, 4) AS Unweighted_Availability_LY,

  -- Findability Last Year
  ROUND(Demand_Weighted_Findability_LY, 4) AS Demand_Weighted_Findability_LY,
  --ROUND(Demand_Weighted_Findability_CG_LY, 4) AS Demand_Weighted_Findability_CG_LY, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_Findability_Stocking_LY, 4) AS Demand_Weighted_Findability_Stocking_LY, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_Findability_Dropship_LY, 4) AS Demand_Weighted_Findability_Dropship_LY,
  --ROUND(Demand_Weighted_Findability_LY - Demand_Weighted_Findability_Dropship_LY) AS Demand_Weighted_Findability_CGAndStocking_LY,
  ROUND(Unweighted_Findability_LY, 4) AS Unweighted_Findability_LY,

  -- In-Stock Rate Last Year
  ROUND(Demand_Weighted_InStockRate_LY, 4) AS Demand_Weighted_InStockRate_LY,
  --ROUND(Demand_Weighted_InStockRate_CG_LY, 4) AS Demand_Weighted_InStockRate_CG_LY, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_InStockRate_Stocking_LY, 4) AS Demand_Weighted_InStockRate_Stocking_LY, -- This metric is commented out because it is zeroed out in the "tbl_agg_retail_availability" table, so would display incorrect values
  --ROUND(Demand_Weighted_InStockRate_Dropship_LY, 4) AS Demand_Weighted_InStockRate_Dropship_LY,
  --ROUND(Demand_Weighted_InStockRate_LY - Demand_Weighted_InStockRate_Dropship_LY, 4) AS Demand_Weighted_InStockRate_CGAndStocking_LY,
  ROUND(Unweighted_InStockRate_LY, 4) AS Unweighted_InStockRate_LY

FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day 
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID

LEFT JOIN (
  SELECT
    PARSE_DATE('%Y%m%d', CAST(retaildate AS STRING)) AS Datum,
    CASE WHEN sty_id = 2 THEN 321 WHEN sty_id = 3 THEN 368 END AS SoID,
    mkc_id AS MkcID,
    cl_id AS ClID,

    -- Availability
    SUM(w_avail_num) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability,
    SUM(w_avail_num_cg) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability_CG,
    SUM(w_avail_num_stocking) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability_Stocking,
    SUM(w_avail_num_ds) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability_Dropship,

    SUM(avail_num) / NULLIF(SUM(denom), 0) AS Unweighted_Availability,

    -- Findability
    SUM(w_find_num) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability,
    SUM(w_find_num_cg) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability_CG,
    SUM(w_find_num_stocking) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability_Stocking,
    SUM(w_find_num_ds) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability_Dropship,

    SUM(find_num) / NULLIF(SUM(denom), 0) AS Unweighted_Findability,
    
    -- In-Stock Rate
    SUM(w_in_stock_num) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate,
    SUM(w_in_stock_num_cg) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate_CG,
    SUM(w_in_stock_num_stocking) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate_Stocking,
    SUM(w_in_stock_num_ds) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate_Dropship,

    SUM(in_stock_num) / NULLIF(SUM(denom), 0) AS Unweighted_InStockRate

  FROM `wf-gcp-us-ae-retail-prod.cm_reporting.tbl_agg_retail_availability` ra
  WHERE 1=1
    AND sty_id IN (2,3) -- 2 for the UK; 3 for DE
    AND PARSE_DATE('%Y%m%d', CAST(retaildate AS STRING)) IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
  GROUP BY 1,2,3,4
) now ON day.Datum = now.Datum AND day.SoID = now.SoID AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID

LEFT JOIN (
  SELECT
    PARSE_DATE('%Y%m%d', CAST(retaildate AS STRING)) AS Datum_LY,
    CASE WHEN sty_id = 2 THEN 321 WHEN sty_id = 3 THEN 368 END AS SoID,
    mkc_id AS MkcID,
    cl_id AS ClID,

    -- Availability
    SUM(w_avail_num) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability_LY,
    SUM(w_avail_num_cg) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability_CG_LY,
    SUM(w_avail_num_stocking) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability_Stocking_LY,
    SUM(w_avail_num_ds) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Availability_Dropship_LY,

    SUM(avail_num) / NULLIF(SUM(denom), 0) AS Unweighted_Availability_LY,

    -- Findability
    SUM(w_find_num) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability_LY,
    SUM(w_find_num_cg) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability_CG_LY,
    SUM(w_find_num_stocking) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability_Stocking_LY,
    SUM(w_find_num_ds) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_Findability_Dropship_LY,

    SUM(find_num) / NULLIF(SUM(denom), 0) AS Unweighted_Findability_LY,
    
    -- In-Stock Rate
    SUM(w_in_stock_num) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate_LY,
    SUM(w_in_stock_num_cg) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate_CG_LY,
    SUM(w_in_stock_num_stocking) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate_Stocking_LY,
    SUM(w_in_stock_num_ds) / NULLIF(SUM(w_denom), 0) AS Demand_Weighted_InStockRate_Dropship_LY,

    SUM(in_stock_num) / NULLIF(SUM(denom), 0) AS Unweighted_InStockRate_LY,

  FROM `wf-gcp-us-ae-retail-prod.cm_reporting.tbl_agg_retail_availability` ra
  WHERE 1=1
    AND sty_id IN (2,3) -- 2 for the UK; 3 for DE
    AND PARSE_DATE('%Y%m%d', CAST(retaildate AS STRING)) IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
  GROUP BY 1,2,3,4
) ly ON day.Datum_LY = ly.Datum_LY AND day.SoID = ly.SoID AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2,7;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.8: Time to Delivery Estimate

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ttd`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ttd`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT
-- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- TTD estimate this year
  Time_to_Delivery_Est_Days,

  -- TTD estimate last year
  Time_to_Delivery_Est_Days_LY
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day 
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID

LEFT JOIN (
  SELECT
    DISTINCT v.SessionStartDate AS Datum,
    v.Event_SoID,
    mkc_2.MkcID,
    cls.ClID,
	PERCENTILE_CONT(DATE_DIFF(CAST(p.DeliveryEstimate AS DATE), v.SessionStartDate, DAY), 0.5) OVER(PARTITION BY v.SessionStartDate,v.Event_SoID,mkc_2.MkcID,cls.ClID) AS Time_to_Delivery_Est_Days -- EDIT ON 12072021 changed the aggregation from avg to median
    --ROUND(AVG(DATE_DIFF(CAST(p.DeliveryEstimate AS DATE), v.SessionStartDate, DAY)), 1) AS Time_to_Delivery_Est_Days -- Calculate the average daily TTD estimate on class level -- EDIT ON 12072021 changed the aggregation from avg to median
  FROM `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits` v 
  INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_clicks_product_request` p
    ON 1=1 
      AND p.SessionStartDate = v.SessionStartDate
      AND p.Event_SoID = v.Event_SoID
      AND p.Event_SessionKey = v.Event_SessionKey
  INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits_exclusion_reason` ex
    ON 1=1
      AND ex.SessionStartDate = v.SessionStartDate 
      AND ex.Event_SoID = v.Event_SoID
      AND ex.Event_SessionKey = v.Event_SessionKey
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_join_product_class` jpc ON p.Event_PrSKU = jpc.PrSKU AND PcMasterClass = TRUE
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON jpc.ClID = cls.ClID
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_marketing_category` mkc_2 ON mkc_2.MkcID = cls.ClMkcID
  WHERE 1=1
    AND v.Event_SoID IN (321,368) -- 321 for the UK, 368 for DE
    AND v.SessionStartDate IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND ex.VisitExclusionreason = 0 -- That's a standard filter to get the right number of visits and views
    AND p.Event_PrSKU IS NOT NULL -- That's a standard filter to get the right number of visits and views
    AND p.DeliveryEstimate > p.SessionStartDate -- Eliminate records where the delivery estimate is less than the session start date (a potential error)
  --GROUP BY 1,2,3,4  -- EDIT ON 12072021 commenting because the aggregation was changed from avg to median
) now ON day.Datum = now.Datum AND day.SoID = now.Event_SoID AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID 

LEFT JOIN (
  SELECT
    DISTINCT v.SessionStartDate AS Datum_LY,
    v.Event_SoID,
    mkc_2.MkcID,
    cls.ClID,
    PERCENTILE_CONT(DATE_DIFF(CAST(p.DeliveryEstimate AS DATE), v.SessionStartDate, DAY), 0.5) OVER(PARTITION BY v.SessionStartDate,v.Event_SoID,mkc_2.MkcID,cls.ClID) AS Time_to_Delivery_Est_Days_LY -- EDIT ON 12072021 changed the aggregation from avg to median
    --ROUND(AVG(DATE_DIFF(CAST(p.DeliveryEstimate AS DATE), v.SessionStartDate, DAY)), 1) AS Time_to_Delivery_Est_Days_LY -- Calculate the average daily TTD estimate on class level -- EDIT ON 12072021 changed the aggregation from avg to median    
  FROM `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits` v 
  INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_clicks_product_request` p
    ON 1=1 
      AND p.SessionStartDate = v.SessionStartDate
      AND p.Event_SoID = v.Event_SoID
      AND p.Event_SessionKey = v.Event_SessionKey
  INNER JOIN `wf-gcp-us-ae-sf-prod.curated_clickstream.tbl_dash_visits_exclusion_reason` ex
    ON 1=1
      AND ex.SessionStartDate = v.SessionStartDate 
      AND ex.Event_SoID = v.Event_SoID
      AND ex.Event_SessionKey = v.Event_SessionKey
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_join_product_class` jpc ON p.Event_PrSKU = jpc.PrSKU AND PcMasterClass = TRUE
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_class` cls ON jpc.ClID = cls.ClID
  INNER JOIN `wf-gcp-us-ae-bulk-prod.csn_product.tbl_marketing_category` mkc_2 ON mkc_2.MkcID = cls.ClMkcID
  WHERE 1=1
    AND v.Event_SoID IN (321,368) -- 321 for the UK, 368 for DE
    AND v.SessionStartDate IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
    AND ex.VisitExclusionreason = 0 -- That's a standard filter to get the right number of visits and views
    AND p.Event_PrSKU IS NOT NULL -- That's a standard filter to get the right number of visits and views
    AND p.DeliveryEstimate > p.SessionStartDate -- Eliminate records where the delivery estimate is less than the session start date (a potential error)
  -- GROUP BY 1,2,3,4  -- EDIT ON 12072021 commenting because the aggregation was changed from avg to median
) ly ON day.Datum_LY = ly.Datum_LY AND day.SoID = ly.Event_SoID AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID 
ORDER BY 1, cl.Mkcname, cl.ClName, 2,7;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.9: Attribute driving tag coverage

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ad_tag_coverage`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ad_tag_coverage`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT
-- Grouping variables (fixed - we LEFT JOIN to them so that we don't have missing records from the original data sources)
  day.SoID,
  day.Datum,
  day.WeekNum,
  day.Day,
  day.Month,
  day.Datum_LY,
  cl.MkcName,
  cl.MkcID,
  cl.ClName,
  cl.ClID,

  -- AD tag coverage estimate this year
  ROUND(ADTagCoverage, 4) AS ADTagCoverage,

  -- AD tag coverage estimate last year
  ROUND(ADTagCoverage_LY, 4) AS ADTagCoverage_LY
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon` day 
CROSS JOIN (
    SELECT DISTINCT 
        MarketingCategoryName AS MkcName, 
        MarketingCategoryID AS MkcID, 
        ClassName AS ClName, 
        ClassID AS ClID 
    FROM `wf-gcp-us-ae-pricing-prod.profit_mgmt.tbl_product_price_report` WHERE BclgID IN (2,3)
) cl -- Creates partitions of SoID-Datum-Datum_LY-MkcName-MkcID-ClName-ClID

LEFT JOIN ( -- This sub-query calculates the attribute driving tag coverage over the last 3 completed months + current MTD + another 30 days
  SELECT
    snapshotdate AS Datum,
    CASE WHEN bclgid = 2 THEN 321 WHEN bclgid = 3 THEN 368 ELSE NULL END AS SoID,
    MkcID,
    ClID,
    SUM(adtagcoveragenumerator) / NULLIF(SUM(adtagcoveragedenominator), 0) AS ADTagCoverage
  FROM `wf-gcp-us-ae-merch-prod.analytics_merch_processing.tbl_bic_merch_product_information_sku`
  WHERE 1=1
    AND bclgid IN (2,3) -- 2 for UK, 3 for DE
    AND snapshotdate IN (SELECT Datum FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
  GROUP BY 1,2,3,4
) now ON day.Datum = now.Datum AND day.SoID = now.SoID AND cl.MkcID = now.MkcID AND cl.ClID = now.ClID

LEFT JOIN (
  SELECT
    snapshotdate AS Datum_LY,
    CASE WHEN bclgid = 2 THEN 321 WHEN bclgid = 3 THEN 368 ELSE NULL END AS SoID,
    MkcID,
    ClID,
    SUM(adtagcoveragenumerator) / NULLIF(SUM(adtagcoveragedenominator), 0) AS ADTagCoverage_LY
  FROM `wf-gcp-us-ae-merch-prod.analytics_merch_processing.tbl_bic_merch_product_information_sku`
  WHERE 1=1
    AND bclgid IN (2,3) -- 2 for UK, 3 for DE
    AND snapshotdate IN (SELECT Datum_LY FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_time_horizon`)
  GROUP BY 1,2,3,4
) ly ON day.Datum_LY = ly.Datum_LY AND day.SoID = ly.SoID AND cl.MkcID = ly.MkcID AND cl.ClID = ly.ClID
ORDER BY 1, cl.Mkcname, cl.ClName, 2,7;

-------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Step 2.10: Combining all metrics together

DROP TABLE IF EXISTS `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_all_metrics`;
CREATE TABLE `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_all_metrics`
OPTIONS(
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
) AS

SELECT
  nonsb.SoID,
  nonsb.Datum,
  nonsb.WeekNum,
  nonsb.Day,
  nonsb.Month,
  nonsb.Datum_LY,
  nonsb.MkcName,
  nonsb.MkcID,
  nonsb.ClName,
  nonsb.ClID,

  -- GRS, total order count, total ordered products, OrderShare, Cart Quantity, AOV, ACO, and AUP this year
  nonsb.GRS,
  nonsb.TotalOrderCount,
  nonsb.TotalOrderedProducts,
  nonsb.OrderShare,
  nonsb.CartQuantity,
  nonsb.Average_Contribution_to_Order,
  nonsb.Average_Order_Value,
  nonsb.Average_Unit_Price,

  -- GRS, total order count, total ordered products, OrderShare, Cart Quantity, AOV, ACO, and AUP over the same time horizon but last year
  nonsb.GRS_LY,
  nonsb.TotalOrderCount_LY,
  nonsb.TotalOrderedProducts_LY,
  nonsb.OrderShare_LY,
  nonsb.CartQuantity_LY,
  nonsb.Average_Contribution_to_Order_LY,
  nonsb.Average_Order_Value_LY,
  nonsb.Average_Unit_Price_LY,

   -- Spread-based GM, Spread-based VCD, Spread-based net revenue, and Spread-based gross profit this year
  sb.Spreadbased_NetRevenue,
  sb.Spreadbased_GrossProfit,
  sb.SpreadBased_GM,
  sb.SpreadBased_GM_Excl_Fulfillment,
  sb.Spreadbased_VCD,
  sb.SpreadBased_VCM,

  -- Spread-based GM, Spread-based VCD, Spread-based net revenue, and Spread-based gross profit over the same time horizon but last year
  sb.Spreadbased_NetRevenue_LY,
  sb.Spreadbased_GrossProfit_LY,
  sb.SpreadBased_GM_LY,
  sb.SpreadBased_GM_Excl_Fulfillment_LY,
  sb.Spreadbased_VCD_LY,
  sb.SpreadBased_VCM_LY,

  -- PPM and AtOrder GM this year
  gm.PPM,
  gm.AtOrderGM,

  -- PPM and AtOrder GM over the same time horizon but last year
  gm.PPM_LY,
  gm.AtOrderGM_LY,

  -- Distinct visits and page views this year
  vis.Visits,
  vis.PageViews, 

  -- Distinct visits and page views last year
  vis.Visits_LY,
  vis.PageViews_LY,

  -- RPV, PPV, and CR this year
  pv.RPV,
  pv.PPV,
  pv.CR,  
  
  -- RPV, PPV, and CR over the same time horizon but last year
  pv.RPV_LY,
  pv.PPV_LY,
  pv.CR_LY,

  -- Indices this year
  rpi.RPI0,
  wpi.WPI0,
  wsi.WSI0,

  rpi.RPI28,
  wpi.WPI28,
  wsi.WSI28,

  -- Indices over the same time horizon but last year
  rpi.RPI0_LY,
  wpi.WPI0_LY,
  wsi.WSI0_LY,

  rpi.RPI28_LY,
  wpi.WPI28_LY,
  wsi.WSI28_LY,

  -- TTD estimate this year
  ttd.Time_to_Delivery_Est_Days,

  -- TTD estimate last year
  ttd.Time_to_Delivery_Est_Days_LY,

  -- AD tag coverage estimate this year
  tag.ADTagCoverage,

  -- AD tag coverage last year
  tag.ADTagCoverage_LY,

  -- Availability, Findability and In-Stock metrics this year (some metrics are commented out because they are zeroed out in the "tbl_agg_retail_availability" table)

  -- Availability
  av.Demand_Weighted_Availability,
  --av.Demand_Weighted_Availability_Dropship,
  --av.Demand_Weighted_Availability_CGAndStocking,
  av.Unweighted_Availability,

  -- Findability
  av.Demand_Weighted_Findability,
  --av.Demand_Weighted_Findability_Dropship,
  --av.Demand_Weighted_Findability_CGAndStocking,
  av.Unweighted_Findability,

  -- In-Stock Rate
  av.Demand_Weighted_InStockRate,
  --av.Demand_Weighted_InStockRate_Dropship,
  --av.Demand_Weighted_InStockRate_CGAndStocking,
  av.Unweighted_InStockRate,

  -- Availability, Findability and In-Stock metrics over the same time period as above BUT LAST YEAR (some metrics are commented out because they are zeroed out in the "tbl_agg_retail_availability" table)

  -- Availability Last Year
  av.Demand_Weighted_Availability_LY,
  --av.Demand_Weighted_Availability_Dropship_LY,
  --av.Demand_Weighted_Availability_CGAndStocking_LY,
  av.Unweighted_Availability_LY,

  -- Findability Last Year
  av.Demand_Weighted_Findability_LY,
  --av.Demand_Weighted_Findability_Dropship_LY,
  --av.Demand_Weighted_Findability_CGAndStocking_LY,
  av.Unweighted_Findability_LY,

  -- In-Stock Rate Last Year
  av.Demand_Weighted_InStockRate_LY,
  --av.Demand_Weighted_InStockRate_Dropship_LY,
  --av.Demand_Weighted_InStockRate_CGAndStocking_LY,
  av.Unweighted_InStockRate_LY, 
  
   -- Date and time stamp (EDIT on 20210702 added date and time stamps)
  CURRENT_DATE() AS DateStamp,
  CURRENT_TIMESTAMP() AS TimeStamp
  
FROM `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_non_sb_metrics` nonsb
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_sb_metrics` sb ON nonsb.Datum = sb.Datum AND nonsb.SoID = sb.SoID AND nonsb.Datum_LY = sb.Datum_LY AND nonsb.MkcID = sb.MkcID AND nonsb.ClID = sb.ClID
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ppm_atordergm` gm ON nonsb.Datum = gm.Datum AND nonsb.SoID = gm.SoID AND nonsb.Datum_LY = gm.Datum_LY AND nonsb.MkcID = gm.MkcID AND nonsb.ClID = gm.ClID
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_visits` vis ON nonsb.Datum = vis.Datum AND nonsb.SoID = vis.SoID AND nonsb.Datum_LY = vis.Datum_LY AND nonsb.MkcID = vis.MkcID AND nonsb.ClID = vis.ClID
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ppv_rpv_cr` pv ON nonsb.Datum = pv.Datum AND nonsb.SoID = pv.SoID AND nonsb.Datum_LY = pv.Datum_LY AND nonsb.MkcID = pv.MkcID AND nonsb.ClID = pv.ClID
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_rpi` rpi ON nonsb.Datum = rpi.Datum AND nonsb.SoID = rpi.SoID AND nonsb.Datum_LY = rpi.Datum_LY AND nonsb.MkcID = rpi.MkcID AND nonsb.ClID = rpi.ClID AND rpi.CompetitorName = 'Amazon Buy Box' -- Need this condition so that the number of rows match the other tables
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_wpi` wpi ON nonsb.Datum = wpi.Datum AND nonsb.SoID = wpi.SoID AND nonsb.Datum_LY = wpi.Datum_LY AND nonsb.MkcID = wpi.MkcID AND nonsb.ClID = wpi.ClID AND wpi.IndexDate = DATE('2019-12-05') -- Need this condition so that the number of rows match the other tables
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_wsi` wsi ON nonsb.Datum = wsi.Datum AND nonsb.SoID = wsi.SoID AND nonsb.Datum_LY = wsi.Datum_LY AND nonsb.MkcID = wsi.MkcID AND nonsb.ClID = wsi.ClID AND wsi.IndexDate = '2019-12-05' -- Need this condition so that the number of rows match the other tables
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_availability` av ON nonsb.Datum = av.Datum AND nonsb.SoID = av.SoID AND nonsb.Datum_LY = av.Datum_LY AND nonsb.MkcID = av.MkcID AND nonsb.ClID = av.ClID
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ttd` ttd ON nonsb.Datum = ttd.Datum AND nonsb.SoID = ttd.SoID AND nonsb.Datum_LY = ttd.Datum_LY AND nonsb.MkcID = ttd.MkcID AND nonsb.ClID = ttd.ClID
LEFT JOIN `wf-gcp-us-ae-pricing-prod.eu_profit_mgmt.class_calc_ad_tag_coverage` tag ON nonsb.Datum = tag.Datum AND nonsb.SoID = tag.SoID AND nonsb.Datum_LY = tag.Datum_LY AND nonsb.MkcID = tag.MkcID AND nonsb.ClID = tag.ClID
ORDER BY 1, nonsb.Mkcname, nonsb.ClName, 2,7;
