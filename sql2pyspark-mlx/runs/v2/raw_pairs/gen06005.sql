SELECT
    id,
    launch_date,
    category,
    cost,
    TRIM(category) AS clean_category
FROM products
WHERE (price <= 1080 AND supplier_id IS NOT NULL) OR launch_date <= '2023-04-21';
