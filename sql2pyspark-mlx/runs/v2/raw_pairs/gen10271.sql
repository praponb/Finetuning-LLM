SELECT carrier, ROUND(STDDEV(cost), 2) stddev_cost, COUNT(*) row_count, AVG(cost) avg_cost FROM shipments GROUP BY carrier ORDER BY avg_cost ASC, carrier ASC LIMIT 5
