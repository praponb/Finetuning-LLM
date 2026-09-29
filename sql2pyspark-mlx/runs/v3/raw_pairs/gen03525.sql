SELECT QUARTER(shipped_date) AS shipped_quarter, SUM(cost) AS total_cost, RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk FROM shipments GROUP BY QUARTER(shipped_date)
