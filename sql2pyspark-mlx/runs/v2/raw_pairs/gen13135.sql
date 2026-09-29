SELECT carrier, min_cost FROM (SELECT carrier, MIN(cost) AS min_cost FROM shipments WHERE shipped_date >= '2024-10-01' GROUP BY carrier) AS agg WHERE min_cost > 41 ORDER BY min_cost, carrier LIMIT 1;
