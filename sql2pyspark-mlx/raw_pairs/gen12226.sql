SELECT id, category, EXPLODE(tags) AS label FROM products
