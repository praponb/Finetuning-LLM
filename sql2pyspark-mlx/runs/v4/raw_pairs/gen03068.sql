SELECT id, category, brand, EXPLODE(tags) AS tag_name FROM products
