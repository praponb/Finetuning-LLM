SELECT id, category, EXPLODE(tags) AS tag_name FROM products
