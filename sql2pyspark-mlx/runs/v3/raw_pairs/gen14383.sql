SELECT carrier, SUM(cost) total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank FROM shipments GROUP BY carrier ORDER BY total_cost ASC
