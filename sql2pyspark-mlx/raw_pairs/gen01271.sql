SELECT product_name, launch_date FROM products WHERE ARRAY_CONTAINS(tags, 'sale') INTERSECT SELECT product_name, launch_date FROM products WHERE launch_date < '2024-12-01' ORDER BY launch_date
