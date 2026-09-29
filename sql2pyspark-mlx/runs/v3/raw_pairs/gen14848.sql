SELECT
    orders.id,
    orders.store_id,
    orders.status,
    orders.shipping_fee
FROM orders
WHERE NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id AND order_items.unit_price BETWEEN 190 AND 210)
ORDER BY id ASC;
