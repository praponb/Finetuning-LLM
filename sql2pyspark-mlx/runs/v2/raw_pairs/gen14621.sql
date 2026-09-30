SELECT id, brand, EXPLODE(tags) AS label FROM products WHERE cost <= 230
