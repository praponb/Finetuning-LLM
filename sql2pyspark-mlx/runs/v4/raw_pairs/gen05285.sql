SELECT carrier, status, avg_cost FROM (SELECT carrier, status, AVG(cost) AS avg_cost FROM shipments GROUP BY carrier, status) AS t WHERE avg_cost >= 27 ORDER BY status;
