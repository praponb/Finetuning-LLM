SELECT id, category, EXPLODE(tags) AS tag FROM products WHERE LOWER(TRIM(product_name)) = 'laptop pro 14';
