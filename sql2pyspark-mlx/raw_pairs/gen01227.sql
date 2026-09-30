WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, unit_price * quantity line_total
    FROM order_items
    WHERE product_id IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        product_id,
        unit_price,
        line_total
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY quantity NULLS LAST, id) = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, line_total * unit_price line_total_times_unit_price
    FROM latest
    WHERE product_id IS NOT NULL
      AND product_id IN (1, 10, 23, 24)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(line_total_times_unit_price) OVER (PARTITION BY product_id) product_id_count_line_total_times_unit_price,
        LAG(line_total_times_unit_price, 1) OVER (PARTITION BY product_id ORDER BY unit_price, id) prev_line_total_times_unit_price
    FROM cleaned
)
-- Grouped metrics
SELECT
    product_id,
    CASE WHEN line_total_times_unit_price >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS line_total_times_unit_price_tier,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(line_total, ARRAY(0.25, 0.5, 0.75)) AS line_total_quartiles,
    SUM(line_total) AS total_line_total,
    COUNT(*) AS row_count,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    PERCENTILE_APPROX(line_total, ARRAY(0.25, 0.5, 0.75)) AS line_total_quartiles_2,
    GROUPING(product_id) AS grouping_id
FROM metrics
GROUP BY GROUPING SETS ((product_id, CASE WHEN line_total_times_unit_price >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END), (product_id))
ORDER BY row_count DESC
