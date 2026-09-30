SELECT category, SUM(stock_qty) AS total_stock_qty, ROUND(SUM(stock_qty) * 100.0 / SUM(SUM(stock_qty)) OVER (), 2) AS pct_of_total
FROM products
WHERE category IN ('Sports', 'Grocery', 'Fashion')
GROUP BY category
ORDER BY category
LIMIT 5;
