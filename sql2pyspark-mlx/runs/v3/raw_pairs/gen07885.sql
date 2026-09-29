SELECT orders.id, orders.shipping_fee FROM orders WHERE NOT EXISTS (SELECT 1 FROM order_items WHERE order_items.order_id = orders.id)
