SELECT id, price, EXPLODE(tags) AS tag FROM products WHERE price BETWEEN 400 AND 500;
