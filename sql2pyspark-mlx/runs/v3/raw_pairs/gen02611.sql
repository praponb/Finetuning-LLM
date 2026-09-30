SELECT
    order_items.id,
    order_items.quantity,
    order_items.product_id,
    products.supplier_id,
    products.product_name,
    orders.store_id
FROM order_items
LEFT JOIN products ON order_items.product_id = products.id
INNER JOIN orders ON orders.id = order_items.order_id
WHERE orders.channel IN ('web', 'mobile', 'partner')
ORDER BY quantity, id
LIMIT 1
