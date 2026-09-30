SELECT oi.id, oi.order_id
FROM order_items oi
WHERE oi.unit_price <= 320 AND oi.order_id IN (SELECT id FROM orders WHERE channel != 'store');
