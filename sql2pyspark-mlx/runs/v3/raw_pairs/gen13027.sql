SELECT brand, COUNT(price) AS price_count, MAX(launch_date) AS latest_launch_date, MAX(stock_qty) - MIN(stock_qty) AS stock_qty_range FROM products GROUP BY brand ORDER BY brand LIMIT 5;
