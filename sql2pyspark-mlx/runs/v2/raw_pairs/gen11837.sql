SELECT tag_name, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name GROUP BY tag_name;
