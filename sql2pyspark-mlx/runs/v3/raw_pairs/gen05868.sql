SELECT s.cost, s.status, s.order_id, o.order_date FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE o.discount <= 0.34 ORDER BY 2;
