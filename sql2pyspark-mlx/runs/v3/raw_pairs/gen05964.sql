SELECT o.status, MIN(s.delivered_date) AS first_delivered_date, SUM(s.cost) AS total_cost FROM shipments s FULL OUTER JOIN orders o ON s.order_id = o.id GROUP BY o.status ORDER BY o.status;
