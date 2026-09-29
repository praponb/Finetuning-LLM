SELECT launch_date, stock_qty FROM products WHERE YEAR(launch_date) >= 2025 UNION SELECT launch_date, stock_qty FROM products WHERE launch_date <= '2023-07-29';
