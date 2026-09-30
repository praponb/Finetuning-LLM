SELECT s.id, s.cost, s.shipped_date FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE discount <= 0.3);
