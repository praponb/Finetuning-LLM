SELECT carrier, MIN(cost) AS min_cost FROM shipments GROUP BY carrier HAVING min_cost < 71
