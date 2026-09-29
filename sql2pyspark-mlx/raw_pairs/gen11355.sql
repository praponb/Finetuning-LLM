WITH product_summary AS (
    SELECT product_id, COUNT(DISTINCT quantity) AS unique_orders, COUNT(quantity) AS quantity_count
    FROM order_items
    GROUP BY product_id
)
SELECT
    p.id,
    p.supplier_id,
    ps.unique_orders,
    ps.quantity_count
FROM products p
INNER JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.unique_orders >= 5
ORDER BY id DESC
