SELECT id, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE launch_date BETWEEN '2023-05-01' AND '2023-06-01';
