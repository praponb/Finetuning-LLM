SELECT id, brand, price, EXPLODE(tags) AS label FROM products WHERE supplier_id IN (1, 8, 16);
