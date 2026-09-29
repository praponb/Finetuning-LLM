WITH product_summary AS (
    SELECT product_id, COUNT(*) AS total_count, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    p.cost,
    ps.total_count,
    ps.stddev_quantity
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.stddev_quantity > 45;
