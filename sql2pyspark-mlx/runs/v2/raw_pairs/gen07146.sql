SELECT id, brand, product_name, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE cost <= 300;
