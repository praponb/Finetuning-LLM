SELECT tag_name, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE ARRAY_CONTAINS(tags, 'eco') GROUP BY tag_name ORDER BY product_count DESC, tag_name;
