SELECT
    products.category,
    products.supplier_id,
    COUNT(order_items.unit_price) AS unit_price_count,
    SUM(order_items.unit_price) AS total_unit_price
FROM order_items
INNER JOIN orders ON order_items.order_id = orders.id
INNER JOIN products ON order_items.product_id = products.id
WHERE order_items.product_id IS NOT NULL
GROUP BY products.category, products.supplier_id
HAVING COUNT(order_items.unit_price) >= 4
