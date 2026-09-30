SELECT launch_date FROM products WHERE product_name IS NOT NULL UNION ALL SELECT launch_date FROM products WHERE launch_date BETWEEN '2025-05-01' AND '2025-06-01' ORDER BY launch_date;
