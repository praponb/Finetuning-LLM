SELECT brand, category, MAX(launch_date) last_launch_date FROM products GROUP BY brand, category;
