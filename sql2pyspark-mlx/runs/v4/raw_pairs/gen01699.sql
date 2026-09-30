WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, quantity - unit_price AS line_total
    FROM order_items
    WHERE quantity IS NOT NULL
)
-- Ranking and gap features
SELECT
    *,
    NTILE(10) OVER (PARTITION BY order_id ORDER BY line_total, id) AS line_total_bucket,
    LAG(line_total, 1) OVER (ORDER BY line_total, id) AS prev_line_total,
    FIRST_VALUE(line_total) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_line_total,
    NTILE(10) OVER (PARTITION BY order_id ORDER BY line_total, id) AS line_total_bucket_2,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY line_total, id) AS event_seq
FROM base
