SELECT id, launch_date
FROM products
WHERE launch_date <= (SELECT MIN(launch_date) FROM products)
ORDER BY id;
