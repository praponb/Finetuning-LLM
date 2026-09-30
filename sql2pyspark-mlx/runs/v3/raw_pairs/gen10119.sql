SELECT o.amount, o.shipping_fee, o.status, oi.quantity, oi.product_id, oi.unit_price FROM orders o LEFT OUTER JOIN order_items oi ON o.id = oi.order_id ORDER BY o.status
