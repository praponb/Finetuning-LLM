SELECT
    product_id,
    store_id,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    MAX(revenue) AS max_revenue
FROM store_sales
GROUP BY 1, 2
ORDER BY avg_revenue, store_id DESC
