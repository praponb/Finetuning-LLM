SELECT category, AVG(cost) AS avg_cost
FROM products
GROUP BY category
