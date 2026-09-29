SELECT id, brand, price, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE price > 1040;
