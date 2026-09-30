SELECT product_name, launch_date FROM products WHERE category != 'Fashion' UNION ALL SELECT product_name, launch_date FROM products WHERE launch_date BETWEEN '2024-09-28' AND '2025-02-01'
