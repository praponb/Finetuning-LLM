SELECT s.id, s.carrier, o.order_date FROM shipments s INNER JOIN orders o ON s.order_id = o.id ORDER BY id DESC;
