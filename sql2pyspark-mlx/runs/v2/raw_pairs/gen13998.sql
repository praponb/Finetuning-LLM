SELECT s.id, s.shipped_date, s.carrier, s.status FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE amount IS NOT NULL) ORDER BY carrier;
