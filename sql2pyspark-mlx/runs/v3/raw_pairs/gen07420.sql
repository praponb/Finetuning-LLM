SELECT
    id,
    product_id,
    revenue,
    sale_date,
    MIN(revenue) OVER (PARTITION BY product_id) AS product_id_min_revenue,
    LAG(revenue, 2) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue
FROM store_sales;
