SELECT id, price, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE supplier_id IN (15, 16, 25, 26)
