WITH kept_keys AS (
    -- Set intersection on keys
    SELECT store_id, product_id
    FROM store_sales
    INTERSECT
    SELECT store_id, product_id
    FROM store_sales
    WHERE store_id = 5
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    INNER JOIN kept_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        sale_date,
        units,
        CAST(revenue AS INT) AS revenue_int
    FROM verified
    WHERE YEAR(sale_date) = 2025
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(revenue_int / units, 2) AS revenue_int_per_units,
        LAST_DAY(sale_date) AS sale_date_month_end
    FROM filtered
    WHERE sale_date > DATE '2023-08-01'
      AND units > 32
),
valid_store_sales AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ADD_MONTHS(sale_date_month_end, 1) AS sale_date_month_end_plus_months,
        ROUND(units, 2) AS units_rounded
    FROM valid
    WHERE sale_date > DATE '2023-06-25'
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid_store_sales
    EXCEPT ALL
    SELECT id
    FROM store_sales
    WHERE product_id IN (2, 5, 10)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_store_sales s
    INNER JOIN matched_keys k ON s.id = k.id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', sale_date) AS activity_month,
    CASE WHEN units_rounded >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_rounded_tier,
    ROUND(AVG(revenue_int_per_units), 2) AS avg_revenue_int_per_units,
    PERCENTILE_APPROX(revenue_int_per_units, ARRAY(0.25, 0.5, 0.75)) AS revenue_int_per_units_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(revenue_int, 0.5) AS revenue_int_p50
FROM reconciled
GROUP BY 1, 2
HAVING APPROX_PERCENTILE(revenue_int, 0.5) > 0
