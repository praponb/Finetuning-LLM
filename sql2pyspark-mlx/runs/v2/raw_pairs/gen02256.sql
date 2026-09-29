SELECT MAX(price) max_price, MAX(launch_date) latest_launch_date, SUM(stock_qty) total_stock_qty FROM products WHERE stock_qty != 348 OR stock_qty BETWEEN 292 AND 396;
