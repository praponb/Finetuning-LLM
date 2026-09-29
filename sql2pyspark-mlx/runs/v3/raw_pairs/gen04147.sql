SELECT status, AVG(cost) AS avg_cost FROM shipments WHERE order_id = 33 GROUP BY 1 HAVING avg_cost >= 27 ORDER BY 2 DESC, status LIMIT 10
