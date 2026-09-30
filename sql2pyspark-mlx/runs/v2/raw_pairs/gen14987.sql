SELECT s.id, s.carrier, s.shipped_date
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE channel NOT IN ('partner', 'web', 'mobile'))
ORDER BY id
