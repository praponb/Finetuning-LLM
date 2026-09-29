SELECT id, product_name, price, EXPLODE(tags) AS label FROM products WHERE launch_date BETWEEN '2025-07-04' AND '2025-08-01';
