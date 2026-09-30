SELECT id, product_name, category, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE supplier_id IS NULL
