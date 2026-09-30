SELECT
    id,
    brand,
    supplier_id,
    launch_date,
    COALESCE(product_name, 'N/A') AS product_name_filled
FROM products
WHERE launch_date < '2024-08-01'
ORDER BY brand, id
LIMIT 5;
