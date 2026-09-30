SELECT orders.id, orders.shipping_fee
FROM orders
WHERE orders.id IN (SELECT order_id FROM order_items WHERE quantity <= 6);
