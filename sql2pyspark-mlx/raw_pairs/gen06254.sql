SELECT id FROM products INTERSECT SELECT id FROM products WHERE LOWER(TRIM(product_name)) = 'backpack xl'
