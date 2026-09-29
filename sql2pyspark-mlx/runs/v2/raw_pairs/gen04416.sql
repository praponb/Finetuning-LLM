WITH product_summary AS (
    SELECT product_id, SUM(unit_price) AS total_unit_price
    FROM order_items
    WHERE order_id IN (5, 17)
    GROUP BY product_id
)
SELECT
    p.id,
    p.launch_date,
    p.supplier_id,
    ps.total_unit_price
FROM products p
JOIN product_summary ps ON p.id = ps.product_id
ORDER BY supplier_id
