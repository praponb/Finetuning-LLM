SELECT
    order_items.id,
    order_items.order_id,
    order_items.product_id,
    products.launch_date,
    products.product_name,
    orders.status
FROM order_items
JOIN products ON products.id = order_items.product_id
LEFT OUTER JOIN orders ON order_items.order_id = orders.id
WHERE order_items.order_id IN (15, 18, 24)
