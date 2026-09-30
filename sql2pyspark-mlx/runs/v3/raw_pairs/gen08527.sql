SELECT
    id,
    cost,
    brand,
    supplier_id,
    CONCAT_WS('/', category, brand) AS category_brand_key
FROM products
WHERE launch_date < '2024-02-01';
