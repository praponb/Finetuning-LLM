SELECT id, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE MONTH(launch_date) IN (4, 9, 11)
