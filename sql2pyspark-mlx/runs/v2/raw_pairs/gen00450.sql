SELECT tag_name, COUNT(*) AS product_count FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE category = 'Books' GROUP BY tag_name;
