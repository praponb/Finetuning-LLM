SELECT id, category, brand, EXPLODE(tags) AS tag FROM products
