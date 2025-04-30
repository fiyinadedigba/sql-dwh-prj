/*
===============================================================================
This are just rough queries written for Quality Checks (same as the quality_checks_silver.sql test script
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/


-- CLEAN THE crm_cust_info TABLE
-- Check for Nulls or Duplicates in the Primary Key
-- Expectation: No Result
SELECT
cst_id,
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;



-- Check for unwanted spaces
-- Expecteation: No Results
SELECT cst_lastname
FROM bronze.crm_cust_info
WHERE cst_lastname != TRIM(cst_lastname)


-- Data standardization & consistency
SELECT DISTINCT cst_marital_status
FROM bronze.crm_cust_info 

-- Check for Nulls or Duplicates in the Primary Key in the bronze.crm_prd_info Table
-- Expectation: No Result
SELECT
prd_id,
COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expecteation: No Results
SELECT prd_nm
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for NULLS or Negative Numbers
-- Expecteation: No Results
SELECT prd_cost
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

-- Data standardization & consistency
SELECT DISTINCT prd_line
FROM bronze.crm_prd_info 

-- Check for invalid date order
SELECT *
FROM bronze.crm_prd_info 
WHERE prd_end_dt < prd_start_dt


-- CLEAN THE crm_prd_info TABLE
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL (prd_cost, 0) AS prd_cost,
CASE UPPER (TRIM(prd_line))
     WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other Sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'n/a'
END AS prd_line,
CAST (prd_start_dt AS DATE) AS prd_start_dt,
CAST (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE ) AS prd_end_date
FROM bronze.crm_prd_info

/* Checking to see which of the newly created prd_key is not 
in the bronze.crm_sales_details table since we will be linking both tables on prd_key */
/* WHERE SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN
(SELECT sls_prd_key FROM bronze.crm_sales_details) */


/* Checking to see which of the newly created cat_id is not 
in the bronze.erp_px_cat_g1v2 table since we will be linking both tables on cat_id */

/*
WHERE REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN
 (SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2)) 
 */



 -- Clean the crm_sales_details dataset
 -- Check for invalid Dtaes in the crm_sales_details
 SELECT 
 NULLIF (sls_ship_dt, 0) sls_order_dt
 FROM  bronze.crm_sales_details
 WHERE sls_ship_dt <= 0 
 OR LEN (sls_ship_dt) != 8
 OR sls_ship_dt > 20500101
 OR sls_ship_dt < 19000101

 -- Check for invalid Dtaes orders

 SELECT 
 *
 FROM silver.crm_sales_details
 WHERE sls_order_dt > sls_ship_dt OR  sls_order_dt > sls_due_dt

-- Check the Data Consistency: Between sales, quantity, and price
-- >> Sales = Quantity * Price
-- >> Values must not be Null, zero, or negative

SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales is NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,   

CASE WHEN sls_price is NULL OR sls_price <=0
        THEN sls_sales / NULLIF(sls_quantity, 0)
     ELSE sls_price
END AS sls_price 
FROM bronze.crm_sales_details

WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
ORDER BY sls_sales, sls_quantity, sls_price 

-- Overall cleaning of the bronze.crm_sales_details
 SELECT
 sls_ord_num,
 sls_prd_key,
 sls_cust_id,
 CASE WHEN sls_order_dt = 0 OR LEN (sls_order_dt) != 8 THEN NULL
      ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
 END AS sls_order_dt,
 CASE WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) != 8 THEN NULL
      ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
 END AS sls_ship_dt,
 CASE WHEN sls_due_dt = 0 OR LEN (sls_due_dt) != 8 THEN NULL
      ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
 END AS sls_due_dt,
 CASE WHEN sls_sales is NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
     ELSE sls_sales
 END AS sls_sales,
 sls_quantity,
 CASE WHEN sls_price is NULL OR sls_price <=0
        THEN sls_sales / NULLIF(sls_quantity, 0)
     ELSE sls_price
 END AS sls_price
 FROM bronze.crm_sales_details



-- Clean the bronze.erp_cust_az12 table

-- Check the link between erp_cust_az12 table and cust info table

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
bdate,
gen
FROM bronze.erp_cust_az12

-- Identify out-of-range dates
SELECT DISTINCT
bdate
FROM bronze.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE()

-- Data standardization & Consistency
SELECT DISTINCT 
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12


--Final cleaning

SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
     ELSE cid
END cid,
CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'n/a'
END AS gen
FROM bronze.erp_cust_az12

-- Quality check of the silver.erp_cust_az12 table
SELECT *
FROM silver.erp_cust_az12

SELECT DISTINCT 
gen
FROM silver.erp_cust_az12



-- Clean the bronze.erp_loc_a101 table
-- Check the cst_key in the silver.crm_cust_info to see it tallies, then ensure it does
SELECT
REPLACE (cid, '-', '') cid,
cntry
FROM bronze.erp_loc_a101

-- Data standardization & consistency

SELECT DISTINCT
cntry AS old_cnt,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN  'n/a'
     ELSE TRIM(cntry)
END cntry

FROM bronze.erp_loc_a101
ORDER BY cntry

-- Quality check of  the bronze.erp_loc_a101 table

SELECT *
FROM silver.erp_loc_a101


-- Clean the bronze.erp_px_cat_g1v2 table
-- CHECK FOR UNWANTED SPACES
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

--Data standardization and consistency
SELECT DISTINCT
maintenance 
FROM bronze.erp_px_cat_g1v2



--Final cleaning of bronze.erp_px_cat_g1v2 table
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2


SELECT *
FROM silver.erp_px_cat_g1v2


EXEC bronze.load_bronze

EXEC silver.load_silver
