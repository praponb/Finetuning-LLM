SELECT id, category, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE launch_date > '2023-12-01';
