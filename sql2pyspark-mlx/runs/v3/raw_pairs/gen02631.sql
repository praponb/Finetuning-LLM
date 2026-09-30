SELECT
    id,
    product_id,
    revenue,
    RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS revenue_rank
FROM store_sales;
