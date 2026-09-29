SELECT carrier, AVG(cost) avg_cost FROM shipments GROUP BY carrier ORDER BY carrier, avg_cost DESC;
