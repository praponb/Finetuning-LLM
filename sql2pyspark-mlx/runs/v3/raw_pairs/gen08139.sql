SELECT id, category, brand, EXPLODE(tags) AS label FROM products
