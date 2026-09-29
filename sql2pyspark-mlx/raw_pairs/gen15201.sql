SELECT id, category, price, EXPLODE(tags) AS label FROM products;
