SELECT id, brand, price, EXPLODE(tags) AS tag_name FROM products
