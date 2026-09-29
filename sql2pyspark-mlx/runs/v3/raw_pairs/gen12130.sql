SELECT s.carrier, s.cost, s.status, o.order_date FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE MONTH(o.order_date) IN (3, 5, 10);
