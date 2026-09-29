WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        brand,
        price,
        cost,
        launch_date,
        LENGTH(product_name) AS product_name_length,
        GREATEST(cost, price) AS greatest_cost_price,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM products
    WHERE brand IS NOT NULL
      AND brand IS DISTINCT FROM 'Initech'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY product_name_length DESC, id ASC) AS rn
    FROM filtered
    QUALIFY rn = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_date_label
    FROM deduped
    WHERE launch_date > '2024-04-01'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.revenue > 3600
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.product_id IN (2, 3, 4, 6)
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, brand
    FROM kept
    EXCEPT ALL
    SELECT id, brand
    FROM products
    WHERE price >= 750
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN remaining_keys k ON s.id = k.id AND s.brand = k.brand
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    CASE WHEN price >= 1040 THEN 'PREMIUM' ELSE 'STANDARD' END AS price_tier,
    DATE_TRUNC('YEAR', launch_date) AS year_start,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT id) AS distinct_ids,
    CORR(cost, price) AS corr_cost_price,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date) ORDER BY CORR(cost, price) DESC) AS corr_cost_price_dense_rank
FROM verified
GROUP BY DATE_TRUNC('WEEK', launch_date), CASE WHEN price >= 1040 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', launch_date)
ORDER BY week_start DESC NULLS FIRST
