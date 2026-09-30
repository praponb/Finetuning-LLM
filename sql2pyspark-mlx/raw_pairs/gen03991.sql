SELECT
    id,
    launch_date,
    category,
    price
FROM products
WHERE launch_date = (SELECT MAX(launch_date) FROM products WHERE supplier_id IN (10, 27, 37))
