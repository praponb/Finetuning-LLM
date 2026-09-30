SELECT id, category, brand, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE cost < 480
