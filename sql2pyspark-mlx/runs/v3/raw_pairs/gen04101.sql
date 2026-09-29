SELECT id, price, EXPLODE(tags) AS tag_name FROM products WHERE MONTH(launch_date) IN (3, 4, 7);
