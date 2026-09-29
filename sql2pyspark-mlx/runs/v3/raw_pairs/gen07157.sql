SELECT brand, YEAR(launch_date) AS launch_year, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range FROM products GROUP BY 1, 2 ORDER BY 1
