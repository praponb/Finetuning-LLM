SELECT
    orders.id,
    orders.order_date,
    orders.channel,
    orders.discount
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id AND order_items.quantity <= 8);
