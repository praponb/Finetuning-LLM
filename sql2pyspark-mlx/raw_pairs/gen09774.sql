SELECT SUM(cost) AS total_cost FROM shipments WHERE cost <= 83 AND shipped_date > '2023-09-01' ORDER BY total_cost DESC;
