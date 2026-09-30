SELECT
    id,
    store_id,
    product_id,
    revenue,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id, product_id), 3) AS revenue_share
FROM store_sales
WHERE units IS NOT NULL;
