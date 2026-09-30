SELECT carrier, SUM(cost) AS total_cost, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk FROM shipments GROUP BY carrier
