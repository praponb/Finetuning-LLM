SELECT QUARTER(launch_date) AS launch_quarter, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty FROM products GROUP BY QUARTER(launch_date)
