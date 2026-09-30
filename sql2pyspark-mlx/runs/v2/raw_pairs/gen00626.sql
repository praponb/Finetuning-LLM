SELECT id, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE cost < 280
