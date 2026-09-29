SELECT
    orders.id,
    orders.channel,
    orders.store_id,
    orders.shipping_fee
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id AND order_items.order_id = 27)
ORDER BY store_id DESC
