WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        product_id,
        unit_price,
        unit_price * quantity AS line_total,
        unit_price + quantity AS line_total_2
    FROM order_items
    WHERE product_id IS NOT NULL
),
filtered_order_items AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(line_total_2 * 100, 2) AS adjusted_line_total_2,
        line_total_2 * unit_price AS line_total_2_times_unit_price
    FROM filtered
    WHERE product_id IS NOT NULL
      AND order_id IN (7, 8, 34)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        line_total_2,
        adjusted_line_total_2,
        line_total_2_times_unit_price,
        AVG(line_total_2) OVER (PARTITION BY order_id, product_id ORDER BY line_total_2, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS line_total_2_avg_5,
        AVG(adjusted_line_total_2) OVER (PARTITION BY order_id ORDER BY line_total_2_times_unit_price, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS adjusted_line_total_2_avg_5
    FROM filtered_order_items
),
with_windows_order_items AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        adjusted_line_total_2,
        line_total_2_times_unit_price,
        line_total_2_avg_5,
        RANK() OVER (PARTITION BY product_id ORDER BY line_total_2) AS line_total_2_rank,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY adjusted_line_total_2_avg_5 NULLS FIRST) AS adjusted_line_total_2_avg_5_dense_rank,
        FIRST_VALUE(line_total_2) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY adjusted_line_total_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_line_total_2
    FROM with_windows
    WHERE line_total_2 BETWEEN 44 AND 51
)
-- Grouped metrics
SELECT
    CASE WHEN line_total_2_times_unit_price >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS line_total_2_times_unit_price_tier,
    product_id,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(adjusted_line_total_2_avg_5_dense_rank), 2) AS avg_adjusted_line_total_2_avg_5_dense_rank,
    COUNT(*) AS row_count
FROM with_windows_order_items
GROUP BY GROUPING SETS ((CASE WHEN line_total_2_times_unit_price >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END, product_id), (CASE WHEN line_total_2_times_unit_price >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY line_total_2_times_unit_price_tier DESC;
