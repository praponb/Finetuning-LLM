SELECT
    id,
    store_id,
    product_id,
    revenue,
    COUNT(*) OVER (PARTITION BY store_id) store_id_count,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 3) revenue_share
FROM store_sales
WHERE product_id = 1
