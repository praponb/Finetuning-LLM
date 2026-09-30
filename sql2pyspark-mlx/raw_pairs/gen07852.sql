WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(units / revenue, 2) revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-04-02'
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id
    FROM base
    EXCEPT ALL
    SELECT product_id
    FROM store_sales
    WHERE product_id IN (1, 4, 9)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.product_id = k.product_id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.sqft,
        p2.opened_date
    FROM reconciled s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, store_id
    FROM with_dims
    INTERSECT ALL
    SELECT id, store_id
    FROM store_sales
    WHERE sale_date BETWEEN '2023-08-01' AND '2024-11-01'
),
reconciled_store_sales AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', sale_date) AS year_start,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(sqft, 0.95) AS sqft_p95,
    COUNT(*) AS row_count,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    MAX(units) AS max_units,
    COUNT(*) AS event_count_2,
    GROUPING(DATE_TRUNC('YEAR', sale_date)) AS grouping_id
FROM reconciled_store_sales
GROUP BY ROLLUP (DATE_TRUNC('YEAR', sale_date))
ORDER BY year_start DESC;
