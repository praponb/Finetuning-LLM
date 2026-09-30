SELECT id, category, price, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE supplier_id IN (9, 27, 29, 31);
