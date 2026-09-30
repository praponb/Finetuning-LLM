SELECT id, stock_qty, tags, MONTH(launch_date) AS launch_date_month FROM products WHERE launch_date BETWEEN '2025-02-01' AND '2025-03-01' AND category = 'Electronics';
