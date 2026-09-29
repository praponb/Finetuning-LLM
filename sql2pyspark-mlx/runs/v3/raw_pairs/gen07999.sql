SELECT id, category, product_name, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE launch_date BETWEEN '2024-01-01' AND '2024-08-20';
