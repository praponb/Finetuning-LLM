SELECT
    category,
    SUM(cost) AS total_cost,
    COUNT(*) AS cnt,
    SUM(SUM(cost)) OVER (ORDER BY category) AS cumulative_cost
FROM products
WHERE stock_qty < 142
GROUP BY category;
