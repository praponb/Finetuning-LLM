SELECT orders.id, orders.customer_id, orders.shipping_fee
FROM orders
WHERE EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id)
ORDER BY customer_id DESC
