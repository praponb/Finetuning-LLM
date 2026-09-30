SELECT category, SUM(cost) AS total_cost, DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank
FROM products
WHERE cost >= 440
GROUP BY category
ORDER BY category DESC;
