SELECT category, SUM(cost) AS total_cost, ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total FROM products GROUP BY category ORDER BY pct_of_total DESC
