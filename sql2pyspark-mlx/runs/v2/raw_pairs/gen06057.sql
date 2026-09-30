SELECT id, category, brand, EXPLODE(tags) AS tag FROM products WHERE category NOT IN ('Home', 'Sports', 'Fashion')
