SELECT id, price, category, EXPLODE(tags) AS label FROM products
