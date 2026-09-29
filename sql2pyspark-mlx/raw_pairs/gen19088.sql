SELECT p.channel, e.carrier, COUNT(*) AS cnt, SUM(e.cost) AS total_cost FROM shipments e JOIN orders p ON e.order_id = p.id GROUP BY p.channel, e.carrier;
