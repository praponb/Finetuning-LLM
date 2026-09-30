SELECT id, product_name, category, EXPLODE(tags) AS tag FROM products
