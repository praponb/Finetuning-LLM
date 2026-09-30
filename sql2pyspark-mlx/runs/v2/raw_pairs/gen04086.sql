SELECT category, MAX(launch_date) last_launch_date, COUNT(CASE WHEN category = 'Fashion' THEN 1 END) fashion_count FROM products GROUP BY 1 ORDER BY category;
