WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        quantity,
        quantity - unit_price AS line_total
    FROM order_items
    WHERE product_id IS NOT NULL
      AND product_id IN (6, 14, 20, 38)
),
base_order_items AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(quantity, line_total) AS greatest_quantity_line_total
    FROM base
    WHERE quantity < 5
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(greatest_quantity_line_total) OVER (ORDER BY greatest_quantity_line_total ASC, id ASC) AS prev_greatest_quantity_line_total,
        LEAD(quantity) OVER (ORDER BY greatest_quantity_line_total ASC, id ASC) AS next_quantity,
        ROW_NUMBER() OVER (ORDER BY greatest_quantity_line_total ASC, id ASC) AS seq_no,
        SUM(greatest_quantity_line_total) OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_greatest_quantity_line_total
    FROM base_order_items
),
base_order_items_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        quantity,
        line_total,
        greatest_quantity_line_total,
        next_quantity,
        seq_no,
        COALESCE(seq_no, 0) AS seq_no_filled
    FROM scored
    WHERE next_quantity >= 7
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        line_total,
        next_quantity,
        seq_no,
        CUME_DIST() OVER (ORDER BY next_quantity ASC NULLS FIRST) AS next_quantity_cume_dist,
        SUM(next_quantity) OVER (ORDER BY greatest_quantity_line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_quantity,
        NTILE(5) OVER (ORDER BY seq_no DESC, id ASC) AS seq_no_bucket,
        AVG(next_quantity) OVER (ORDER BY greatest_quantity_line_total ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_quantity_avg_5_prior
    FROM base_order_items_2
)
-- Multi-level aggregation
SELECT
    CASE WHEN running_next_quantity >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_next_quantity_tier,
    CASE WHEN next_quantity_cume_dist >= 0.72 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_quantity_cume_dist_tier,
    SUM(seq_no_bucket) AS total_seq_no_bucket,
    ROUND(AVG(next_quantity), 2) AS avg_next_quantity,
    COUNT(*) AS event_count,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY CUBE (CASE WHEN running_next_quantity >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN next_quantity_cume_dist >= 0.72 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING ROUND(AVG(next_quantity), 2) > 0;
