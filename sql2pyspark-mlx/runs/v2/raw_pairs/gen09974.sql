SELECT id, product_name, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE price BETWEEN 490 AND 890;
