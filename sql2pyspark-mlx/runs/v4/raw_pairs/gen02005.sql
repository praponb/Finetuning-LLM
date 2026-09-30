SELECT id, category, price, EXPLODE(tags) AS tag FROM products;
