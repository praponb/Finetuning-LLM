SELECT id, product_name, price, EXPLODE(tags) AS tag FROM products
