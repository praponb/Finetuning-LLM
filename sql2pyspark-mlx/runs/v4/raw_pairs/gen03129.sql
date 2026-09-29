SELECT
    CASE WHEN revenue >= 3500 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    CORR(units, revenue) AS corr_units_revenue,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    APPROX_PERCENTILE(revenue, 0.5) AS revenue_p50
FROM store_sales
GROUP BY CASE WHEN revenue >= 3500 THEN 'PREMIUM' ELSE 'STANDARD' END
