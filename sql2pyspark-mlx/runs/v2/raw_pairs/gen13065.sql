SELECT id, price, brand, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE supplier_id IN (27, 37)
