SELECT id, brand, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label
