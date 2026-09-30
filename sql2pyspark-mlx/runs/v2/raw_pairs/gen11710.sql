SELECT status, COUNT(cost) AS cost_count, AVG(cost) AS avg_cost FROM shipments WHERE order_id = 15 AND shipped_date BETWEEN '2023-11-25' AND '2024-05-01' GROUP BY status HAVING avg_cost >= 90;
