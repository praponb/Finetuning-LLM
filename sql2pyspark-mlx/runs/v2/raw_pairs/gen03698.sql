SELECT
    id,
    product_id,
    revenue,
    store_id,
    MIN(revenue) OVER (PARTITION BY product_id) AS product_id_min_revenue,
    RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS rnk
FROM store_sales
