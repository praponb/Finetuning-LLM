SELECT
    category,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS cost_rank
FROM products
GROUP BY category
