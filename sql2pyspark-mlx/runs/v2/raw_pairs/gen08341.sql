SELECT tag, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE UPPER(product_name) LIKE '%IRE%' GROUP BY tag;
