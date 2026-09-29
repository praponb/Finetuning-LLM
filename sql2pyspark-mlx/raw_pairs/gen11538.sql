SELECT id, category, product_name, EXPLODE(tags) AS tag_name FROM products
