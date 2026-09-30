SELECT id, brand, category, label FROM products LATERAL VIEW EXPLODE(tags) t AS label WHERE launch_date <= '2023-04-01';
