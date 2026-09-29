SELECT carrier, avg_cost FROM (SELECT carrier, ROUND(AVG(cost), 2) AS avg_cost FROM shipments GROUP BY carrier) AS x WHERE avg_cost < 29;
