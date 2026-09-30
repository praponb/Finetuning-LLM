SELECT
    orders.id,
    orders.order_date,
    orders.shipping_fee,
    orders.store_id
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id);
