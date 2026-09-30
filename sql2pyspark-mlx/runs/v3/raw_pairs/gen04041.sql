SELECT s.id, s.status, o.discount FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE s.cost BETWEEN 42 AND 58;
