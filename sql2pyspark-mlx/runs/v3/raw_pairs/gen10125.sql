SELECT MONTH(shipped_date) AS shipped_month, SUM(cost) AS total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank FROM shipments GROUP BY MONTH(shipped_date) ORDER BY shipped_month LIMIT 10
