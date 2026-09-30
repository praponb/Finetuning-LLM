SELECT category, SUM(cost) AS total_cost, SUM(SUM(cost)) OVER (ORDER BY category) AS cumulative_cost
FROM products
WHERE cost < 660
GROUP BY category;
