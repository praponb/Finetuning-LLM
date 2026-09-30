SELECT category, ROUND(STDDEV(stock_qty), 2) stddev_stock_qty, AVG(stock_qty) avg_stock_qty
FROM products
GROUP BY category
