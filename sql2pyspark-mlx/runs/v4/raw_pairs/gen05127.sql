SELECT
    id,
    revenue,
    units,
    store_id,
    ROUND(revenue * 1.15, 2) AS revenue_scaled
FROM store_sales
