SELECT tag_name, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE supplier_id IS NULL GROUP BY tag_name;
