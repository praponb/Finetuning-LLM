SELECT id, category, price, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE cost <= 690
