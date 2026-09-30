SELECT id, launch_date, category
FROM products
WHERE launch_date < (SELECT MIN(launch_date) FROM products WHERE supplier_id IS NOT NULL)
ORDER BY id ASC;
