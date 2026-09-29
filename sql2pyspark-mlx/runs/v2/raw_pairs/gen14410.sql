SELECT
    id,
    product_id,
    revenue,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 3) AS revenue_share
FROM store_sales
