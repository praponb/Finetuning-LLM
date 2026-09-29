WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(quantity * 0.85, 2) AS quantity_scaled
    FROM order_items
    WHERE quantity >= 5
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY unit_price, id) AS rn
    FROM filtered
    QUALIFY rn = 1
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT product_id
    FROM latest
    INTERSECT
    SELECT product_id
    FROM order_items
    WHERE product_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN remaining_keys k ON s.product_id = k.product_id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        quantity,
        quantity_scaled,
        CAST(quantity AS STRING) AS quantity_num
    FROM verified
    WHERE product_id IS NOT NULL
      AND unit_price < 270
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN quantity_scaled >= 73 THEN 'LARGE' ELSE 'SMALL' END AS quantity_scaled_tier,
    CASE WHEN quantity >= 7 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    ROUND(AVG(quantity), 2) AS avg_quantity,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(quantity, 0.5) AS quantity_p50
FROM valid
GROUP BY 1, 2
ORDER BY quantity_tier;
