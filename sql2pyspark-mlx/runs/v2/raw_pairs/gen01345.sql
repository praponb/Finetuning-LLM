SELECT id, product_name, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE category IN ('Home', 'Books')
