SELECT id, product_name, EXPLODE(tags) AS tag FROM products WHERE supplier_id IN (21, 25)
