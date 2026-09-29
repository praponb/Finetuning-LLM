SELECT
    category,
    price,
    product_name,
    DAY(launch_date) AS launch_date_day
FROM products
ORDER BY product_name
