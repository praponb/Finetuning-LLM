SELECT o.id, o.store_id, o.channel
FROM orders o
WHERE o.store_id IS NULL AND NOT EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND MONTH(s.shipped_date) IN (5, 9, 12))
ORDER BY channel ASC
