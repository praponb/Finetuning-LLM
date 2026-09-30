SELECT MONTH(launch_date) AS launch_month, category, ROUND(AVG(stock_qty), 2) AS avg_stock_qty, AVG(price) AS avg_price FROM products GROUP BY MONTH(launch_date), category ORDER BY avg_price DESC
