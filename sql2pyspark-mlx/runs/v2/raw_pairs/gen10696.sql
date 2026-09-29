SELECT id, category, launch_date, RANK() OVER (PARTITION BY category ORDER BY launch_date) AS rnk FROM products WHERE LOWER(TRIM(product_name)) = 'backpack xl'
