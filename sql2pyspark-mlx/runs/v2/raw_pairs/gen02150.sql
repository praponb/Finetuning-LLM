SELECT tag_name, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE cost < 670 GROUP BY tag_name ORDER BY product_count DESC, tag_name
