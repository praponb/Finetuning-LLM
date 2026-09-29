WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(category, ' ', product_name) AS category_product_name,
        COALESCE(brand, 'OTHER') AS resolved_brand
    FROM products
    WHERE supplier_id IS NOT NULL
      AND stock_qty BETWEEN 117 AND 328
),
cleaned_products AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        resolved_brand,
        DATE_TRUNC('week', launch_date) AS launch_date_period
    FROM cleaned
    WHERE brand IS DISTINCT FROM 'Globex'
      AND brand IS NOT NULL
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(launch_date_period) OVER (PARTITION BY category ORDER BY launch_date_period, id) IS NULL
                 OR launch_date_period > LAG(launch_date_period) OVER (PARTITION BY category ORDER BY launch_date_period, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM cleaned_products
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY category ORDER BY launch_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT brand
    FROM sessionized
    EXCEPT DISTINCT
    SELECT brand
    FROM products
    WHERE brand IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessionized s
    INNER JOIN kept_keys k ON s.brand = k.brand
),
enriched AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY brand ORDER BY stock_qty NULLS LAST) AS stock_qty_cume_dist
    FROM verified
)
SELECT
    category,
    brand,
    stock_qty,
    tags,
    supplier_id,
    island_id,
    stock_qty_cume_dist,
    CASE WHEN category RLIKE '^(Fashion|Grocery)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS category_zone
FROM enriched
ORDER BY island_id DESC NULLS LAST;
