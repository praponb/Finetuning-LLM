SELECT oi.id, oi.unit_price, o.shipping_fee
FROM order_items oi
INNER JOIN orders o ON o.id = oi.order_id
WHERE o.channel IS NULL AND oi.unit_price <= 280
