SELECT category, AVG(cost) AS avg_cost, MIN(stock_qty) AS min_stock_qty
FROM products
WHERE SIZE(tags) > 2
GROUP BY category
HAVING SUM(price) > 5500
ORDER BY avg_cost DESC, category DESC;
