SELECT category, SUM(stock_qty) total_stock_qty, SUM(SUM(stock_qty)) OVER (ORDER BY category) AS cumulative_stock_qty
FROM products
WHERE brand = 'Globex'
GROUP BY category
ORDER BY category
LIMIT 5
