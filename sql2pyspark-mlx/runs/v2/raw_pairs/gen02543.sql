SELECT s.id, s.delivered_date, s.shipped_date
FROM shipments s
WHERE s.status = 'RETURNED' AND s.order_id IN (SELECT id FROM orders WHERE store_id IS NULL);
