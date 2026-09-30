SELECT s.id, s.shipped_date
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE amount >= 610)
ORDER BY shipped_date, id
LIMIT 10;
