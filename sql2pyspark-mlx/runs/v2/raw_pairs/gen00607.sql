SELECT carrier, MAX(cost) AS max_cost FROM shipments GROUP BY 1 HAVING MAX(cost) > 79
