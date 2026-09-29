SELECT id, product_name, brand, EXPLODE(tags) AS tag_name FROM products WHERE launch_date >= '2023-10-20'
