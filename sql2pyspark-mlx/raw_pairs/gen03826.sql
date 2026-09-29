SELECT id, launch_date, cost
FROM products
WHERE launch_date > (SELECT MAX(launch_date) FROM products WHERE SIZE(tags) > 2)
