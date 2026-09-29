SELECT id, price, category, EXPLODE(tags) AS tag FROM products WHERE supplier_id IN (13, 21);
