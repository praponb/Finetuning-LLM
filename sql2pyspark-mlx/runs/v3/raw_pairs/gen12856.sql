SELECT id, launch_date, product_name, category FROM products WHERE brand = 'Umbrella' AND LOWER(product_name) LIKE '%oga%' ORDER BY launch_date, id LIMIT 3
