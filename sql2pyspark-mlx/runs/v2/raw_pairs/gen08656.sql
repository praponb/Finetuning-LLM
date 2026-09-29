SELECT id, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE price BETWEEN 870 AND 920
