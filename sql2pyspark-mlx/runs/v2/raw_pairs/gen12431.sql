SELECT id, product_name, EXPLODE(tags) AS label FROM products WHERE supplier_id IN (3, 10, 33)
