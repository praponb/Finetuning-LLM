SELECT id, category, tag_name FROM products LATERAL VIEW EXPLODE(tags) t AS tag_name WHERE YEAR(launch_date) >= 2024;
