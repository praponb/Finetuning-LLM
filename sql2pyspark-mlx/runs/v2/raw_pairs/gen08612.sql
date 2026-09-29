SELECT
    category,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    DENSE_RANK() OVER (ORDER BY SUM(cost) DESC) AS rnk
FROM products
WHERE cost <= 650
GROUP BY category
ORDER BY rnk DESC;
