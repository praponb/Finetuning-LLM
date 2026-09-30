SELECT MAX(launch_date) AS last_launch_date
FROM products
WHERE supplier_id IS NOT NULL
