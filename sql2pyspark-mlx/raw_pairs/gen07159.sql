SELECT id, carrier, CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END is_delivered
FROM shipments
WHERE (order_id IS NULL AND carrier = 'Kerry') OR status = 'LOST'
ORDER BY id DESC;
