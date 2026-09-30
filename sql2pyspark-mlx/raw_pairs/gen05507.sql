SELECT
    id,
    tags,
    cost,
    price
FROM products
WHERE MONTH(launch_date) IN (1, 7, 10);
