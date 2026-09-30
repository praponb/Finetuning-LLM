SELECT category, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty FROM products WHERE cost > 410 GROUP BY category HAVING stddev_stock_qty > 61 ORDER BY stddev_stock_qty DESC, 1 DESC
