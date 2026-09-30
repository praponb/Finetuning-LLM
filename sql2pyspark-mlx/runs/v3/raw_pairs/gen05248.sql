SELECT s.id, s.cost, s.order_id, o.status, o.channel FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE o.order_date <= '2024-12-01';
