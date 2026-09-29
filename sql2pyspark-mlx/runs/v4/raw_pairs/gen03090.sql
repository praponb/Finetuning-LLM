SELECT id, brand, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE price >= 850
