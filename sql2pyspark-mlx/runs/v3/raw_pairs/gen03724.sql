SELECT id, brand, category, EXPLODE(tags) AS tag_name FROM products
