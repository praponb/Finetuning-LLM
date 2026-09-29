SELECT id, category, product_name, EXPLODE(tags) AS label FROM products WHERE launch_date > '2025-04-01'
