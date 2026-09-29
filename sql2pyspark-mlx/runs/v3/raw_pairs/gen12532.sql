SELECT category, MONTH(launch_date) AS launch_month, ROUND(STDDEV(stock_qty), 2) AS stddev_stock_qty
FROM products
WHERE price >= 1170 AND supplier_id IS NULL
GROUP BY CUBE(category, MONTH(launch_date))
