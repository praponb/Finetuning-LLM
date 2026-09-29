SELECT
    id,
    product_id,
    sale_date,
    revenue,
    SUM(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_revenue,
    RANK() OVER (PARTITION BY product_id ORDER BY revenue) AS revenue_rank
FROM store_sales;
