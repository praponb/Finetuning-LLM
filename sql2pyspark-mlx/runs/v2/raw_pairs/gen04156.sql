SELECT id, category, price, EXPLODE(tags) AS label FROM products WHERE supplier_id IN (27, 28)
