SELECT id, price, product_name, EXPLODE(tags) AS tag FROM products WHERE price > 1050;
