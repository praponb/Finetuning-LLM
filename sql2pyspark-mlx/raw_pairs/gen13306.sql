SELECT id, brand, category, EXPLODE(tags) AS label FROM products;
