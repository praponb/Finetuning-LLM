SELECT MAX(launch_date) AS last_launch_date, MIN(cost) AS min_cost
FROM products
WHERE LENGTH(product_name) >= 13;
