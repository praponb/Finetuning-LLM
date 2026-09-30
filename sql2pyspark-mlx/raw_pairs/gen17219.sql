SELECT
    id,
    supplier_id,
    tags,
    product_name,
    DATE_ADD(launch_date, 23) AS launch_date_plus_23d
FROM products
WHERE launch_date <= '2025-08-01';
