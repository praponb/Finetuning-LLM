SELECT orders.id, orders.store_id, orders.order_date FROM orders WHERE orders.shipping_fee BETWEEN 16 AND 17 AND NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id)
