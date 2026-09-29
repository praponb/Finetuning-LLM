SELECT s.id, s.cost, s.carrier, s.order_id, o.customer_id, o.discount FROM shipments s INNER JOIN orders o ON s.order_id = o.id;
