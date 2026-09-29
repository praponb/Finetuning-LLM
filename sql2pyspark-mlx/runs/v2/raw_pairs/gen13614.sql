SELECT id, product_name, brand, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag;
