SELECT s.id, s.delivered_date, s.status, s.shipped_date FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE amount > 1180) ORDER BY s.delivered_date, id
