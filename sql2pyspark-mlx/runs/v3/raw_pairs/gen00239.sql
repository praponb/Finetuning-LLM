SELECT QUARTER(shipped_date) AS shipped_quarter, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY QUARTER(shipped_date)) AS cumulative_cost
FROM shipments
GROUP BY QUARTER(shipped_date)
ORDER BY cumulative_cost, shipped_quarter;
