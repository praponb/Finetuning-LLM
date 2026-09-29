SELECT id, brand, EXPLODE(tags) AS tag_name FROM products WHERE supplier_id IN (6, 30, 31);
