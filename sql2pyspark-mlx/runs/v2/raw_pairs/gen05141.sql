SELECT category, SUM(cost) AS total_cost FROM products GROUP BY category ORDER BY total_cost DESC, category
