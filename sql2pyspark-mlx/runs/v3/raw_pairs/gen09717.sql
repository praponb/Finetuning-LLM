SELECT
    product_name,
    launch_date,
    tags,
    INITCAP(TRIM(brand)) AS brand_title
FROM products
WHERE (supplier_id IS NULL AND supplier_id IN (4, 18, 32)) OR product_name LIKE 'U%'
