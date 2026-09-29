SELECT MONTH(launch_date) launch_month, SUM(price) total_price, ROUND(SUM(price) * 100.0 / SUM(SUM(price)) OVER (), 2) AS pct_of_total FROM products WHERE stock_qty < 286 GROUP BY MONTH(launch_date);
