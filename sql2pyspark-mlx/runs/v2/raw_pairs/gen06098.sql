SELECT s.id, s.cost, s.shipped_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE amount >= 1060)
