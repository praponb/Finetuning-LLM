SELECT id, category, EXPLODE(tags) AS tag FROM products;
