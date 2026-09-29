SELECT o.store_id, SUM(s.cost) AS total_cost FROM shipments s JOIN orders o ON o.id = s.order_id WHERE MONTH(s.shipped_date) IN (1, 6, 7) GROUP BY o.store_id ORDER BY total_cost DESC;
