SELECT
    id,
    product_id,
    revenue,
    AVG(revenue) OVER (PARTITION BY product_id) product_id_avg_revenue
FROM store_sales
ORDER BY id DESC;
