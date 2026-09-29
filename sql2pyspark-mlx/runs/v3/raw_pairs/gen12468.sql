SELECT id, product_name, brand, EXPLODE(tags) AS tag FROM products
