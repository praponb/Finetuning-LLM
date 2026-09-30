SELECT carrier, COUNT(CASE WHEN carrier = 'UPS' THEN 1 END) AS ups_count, ROUND(AVG(cost), 2) AS avg_cost FROM shipments GROUP BY carrier HAVING ups_count > 1 ORDER BY ups_count DESC
