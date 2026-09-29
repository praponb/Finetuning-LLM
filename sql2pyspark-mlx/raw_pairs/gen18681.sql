SELECT id, category, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE product_name IS NOT NULL
