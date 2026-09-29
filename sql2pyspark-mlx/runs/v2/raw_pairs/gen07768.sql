SELECT
    id,
    store_id,
    revenue,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 3) AS revenue_share
FROM store_sales
WHERE product_id IN (4, 5, 6, 8)
ORDER BY store_id, id
