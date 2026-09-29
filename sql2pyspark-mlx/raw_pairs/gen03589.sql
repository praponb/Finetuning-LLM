SELECT
    CASE WHEN units >= 39 THEN 'LARGE' ELSE 'SMALL' END AS units_tier,
    product_id,
    CORR(units, revenue) AS corr_units_revenue,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(units, 0.95) AS units_p95,
    APPROX_PERCENTILE(units, 0.95) AS units_p95_2,
    COUNT(*) AS num_rows
FROM store_sales
GROUP BY CASE WHEN units >= 39 THEN 'LARGE' ELSE 'SMALL' END, product_id
ORDER BY cnt;
