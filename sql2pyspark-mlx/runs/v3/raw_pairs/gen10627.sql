SELECT carrier, SUM(cost) total_cost, SUM(SUM(cost)) OVER (ORDER BY carrier) AS cumulative_cost FROM shipments GROUP BY carrier
