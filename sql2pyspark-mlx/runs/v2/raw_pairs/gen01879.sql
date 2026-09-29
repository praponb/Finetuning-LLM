SELECT carrier, SUM(cost) total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank FROM shipments WHERE order_id = 17 GROUP BY carrier;
