SELECT oi.id, oi.order_id, oi.product_id, oi.quantity, o.shipping_fee, o.amount, o.customer_id FROM order_items oi LEFT JOIN orders o ON oi.order_id = o.id
