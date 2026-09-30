SELECT
    id,
    store_id,
    revenue,
    MIN(revenue) OVER (PARTITION BY store_id) AS store_id_min_revenue
FROM store_sales
WHERE units != 39;
