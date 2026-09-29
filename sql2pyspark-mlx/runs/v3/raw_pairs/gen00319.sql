SELECT id, brand, price, EXPLODE(tags) AS tag FROM products
