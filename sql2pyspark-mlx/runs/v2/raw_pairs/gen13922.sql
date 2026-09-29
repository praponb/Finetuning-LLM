SELECT MONTH(launch_date) launch_month, COUNT(DISTINCT stock_qty) unique_suppliers FROM products WHERE launch_date >= '2025-01-10' GROUP BY MONTH(launch_date)
