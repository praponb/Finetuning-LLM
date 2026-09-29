SELECT id, product_name, EXPLODE(tags) AS tag FROM products WHERE brand = 'Globex';
