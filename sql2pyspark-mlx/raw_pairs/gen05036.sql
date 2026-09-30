SELECT s.id, s.shipped_date, s.status, o.order_date FROM shipments s RIGHT JOIN orders o ON s.order_id = o.id;
