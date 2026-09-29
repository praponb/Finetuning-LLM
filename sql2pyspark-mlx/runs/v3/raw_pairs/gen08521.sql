SELECT oi.id, oi.order_id, oi.quantity, o.shipping_fee, o.status, o.channel FROM order_items oi JOIN orders o ON oi.order_id = o.id
