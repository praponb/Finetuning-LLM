SELECT
    id,
    launch_date,
    cost,
    product_name
FROM products
WHERE launch_date = (SELECT MIN(launch_date) FROM products);
