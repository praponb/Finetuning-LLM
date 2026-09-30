SELECT id, price, EXPLODE(tags) AS tag FROM products;
