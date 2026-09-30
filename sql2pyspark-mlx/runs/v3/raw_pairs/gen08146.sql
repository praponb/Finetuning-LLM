SELECT
    o.id,
    o.status,
    o.store_id,
    o.amount
FROM orders o
WHERE o.amount < 1210 AND NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.unit_price <= 140)
ORDER BY id DESC
