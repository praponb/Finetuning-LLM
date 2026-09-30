SELECT id, brand, price, EXPLODE(tags) AS label FROM products;
