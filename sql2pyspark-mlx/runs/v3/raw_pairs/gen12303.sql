SELECT id, product_name, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE cost >= 640
