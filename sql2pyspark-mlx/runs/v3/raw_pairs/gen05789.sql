SELECT category, SUM(stock_qty) AS total_stock_qty, COUNT(stock_qty) AS stock_qty_count
FROM products
GROUP BY category
ORDER BY stock_qty_count DESC;
