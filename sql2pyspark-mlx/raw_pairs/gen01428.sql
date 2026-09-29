SELECT id, category, price, label FROM products LATERAL VIEW EXPLODE(tags) t AS label;
