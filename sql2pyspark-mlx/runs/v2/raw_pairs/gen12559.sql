SELECT
    id,
    store_id,
    revenue,
    product_id
FROM store_sales
WHERE sale_date BETWEEN '2025-02-01' AND '2025-02-12'
