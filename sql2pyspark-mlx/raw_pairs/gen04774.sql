SELECT
    id,
    sale_date,
    product_id,
    revenue,
    DENSE_RANK() OVER (ORDER BY sale_date) AS sale_date_dense_rank,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 3) AS revenue_share
FROM store_sales
WHERE units = 12
ORDER BY revenue
