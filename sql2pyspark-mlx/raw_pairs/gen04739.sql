SELECT id, brand, EXPLODE(tags) AS label FROM products;
