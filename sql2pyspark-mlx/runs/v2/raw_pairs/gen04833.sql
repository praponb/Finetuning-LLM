SELECT id, product_name, category, EXPLODE(tags) AS tag_name FROM products
