WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(unit_price, 0) unit_price_filled,
        CAST(quantity AS STRING) quantity_num
    FROM order_items
    WHERE order_id IN (2, 40)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(unit_price) unit_price_whole,
        ROUND(unit_price * 1.1, 2) unit_price_adjusted
    FROM cleaned
    WHERE product_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(unit_price_adjusted) OVER (PARTITION BY order_id ORDER BY unit_price_adjusted, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_unit_price_adjusted,
        LEAD(quantity, 1) OVER (PARTITION BY order_id ORDER BY unit_price_whole, id) next_quantity,
        NTILE(3) OVER (PARTITION BY order_id, product_id ORDER BY unit_price DESC, id) unit_price_bucket
    FROM valid
    WHERE unit_price_filled BETWEEN 38 AND 75
),
kept_keys AS (
    -- Set difference on keys
    SELECT order_id, id
    FROM metrics
    EXCEPT
    SELECT order_id, id
    FROM order_items
    WHERE order_id = 13
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.order_id = k.order_id AND s.id = k.id
)
-- Grouped metrics
SELECT
    order_id,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(unit_price_filled, ARRAY(0.25, 0.5, 0.75)) AS unit_price_filled_quartiles
FROM verified
GROUP BY 1
