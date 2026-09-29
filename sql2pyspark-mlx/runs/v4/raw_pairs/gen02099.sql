WITH base AS (
    SELECT *, unit_price - quantity AS line_total
    FROM order_items
    WHERE quantity <= 8 AND (order_id IN (11, 17, 25, 29) OR order_id IN (25, 40))
),
agg AS (
    SELECT product_id, COUNT(*) AS row_count, ROUND(STDDEV(line_total), 2) AS stddev_line_total
    FROM base
    GROUP BY product_id
)
SELECT product_id, row_count, stddev_line_total
FROM agg
ORDER BY product_id, stddev_line_total
LIMIT 3
