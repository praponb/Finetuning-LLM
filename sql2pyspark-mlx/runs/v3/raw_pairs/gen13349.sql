SELECT carrier, ROUND(AVG(cost), 2) avg_cost FROM shipments GROUP BY carrier HAVING avg_cost > 75 ORDER BY carrier ASC;
