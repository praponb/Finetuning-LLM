SELECT
    id,
    product_id,
    revenue,
    sale_date,
    DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) AS dense_rnk,
    LAG(revenue, 1) OVER (PARTITION BY product_id ORDER BY sale_date, id) AS prev_revenue
FROM store_sales
