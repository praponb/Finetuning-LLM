SELECT
    id,
    product_id,
    revenue,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS revenue_dense_rank
FROM store_sales
