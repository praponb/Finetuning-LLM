SELECT id, product_name
FROM products
WHERE cost > 690 AND MONTH(launch_date) IN (3, 8, 10)
