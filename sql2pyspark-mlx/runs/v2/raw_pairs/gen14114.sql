SELECT id, price, brand, tag FROM products LATERAL VIEW EXPLODE(tags) t AS tag WHERE MONTH(launch_date) IN (1, 8, 10);
