SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, category, ROUND(AVG(price), 2) AS avg_price FROM products WHERE stock_qty BETWEEN 112 AND 209 GROUP BY launch_month, category
