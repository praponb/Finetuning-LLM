SELECT category, ROUND(AVG(stock_qty), 2) AS avg_stock_qty
FROM products
WHERE stock_qty <> 270 AND category = 'Grocery'
GROUP BY category
HAVING ROUND(AVG(stock_qty), 2) < 52
ORDER BY avg_stock_qty DESC;
