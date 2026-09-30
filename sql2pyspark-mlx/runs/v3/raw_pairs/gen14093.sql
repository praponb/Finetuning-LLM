SELECT YEAR(launch_date) AS launch_year, SUM(stock_qty) AS total_stock_qty, SUM(SUM(stock_qty)) OVER (ORDER BY YEAR(launch_date)) AS cumulative_stock_qty FROM products GROUP BY YEAR(launch_date);
