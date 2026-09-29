SELECT
    product_id,
    DATE_TRUNC('WEEK', sale_date) AS week_start,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    PERCENTILE_APPROX(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(DISTINCT store_id) AS distinct_store_ids
FROM store_sales
GROUP BY product_id, DATE_TRUNC('WEEK', sale_date)
ORDER BY distinct_store_ids DESC;
