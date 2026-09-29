SELECT supplier_id, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty, AVG(stock_qty) AS avg_stock_qty FROM products GROUP BY supplier_id ORDER BY supplier_id;
