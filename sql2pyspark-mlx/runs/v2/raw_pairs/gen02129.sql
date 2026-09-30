SELECT id, product_name, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE launch_date BETWEEN '2024-02-01' AND '2024-06-01'
