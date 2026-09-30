SELECT o.id, o.shipping_fee, t.latest_paid_at FROM orders o JOIN (SELECT order_id, MAX(paid_at) AS latest_paid_at FROM payments GROUP BY order_id) t ON o.id = t.order_id ORDER BY id DESC, 3
