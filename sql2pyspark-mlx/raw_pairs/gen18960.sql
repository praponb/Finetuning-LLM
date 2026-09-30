SELECT id, price, brand, EXPLODE(tags) AS label FROM products;
