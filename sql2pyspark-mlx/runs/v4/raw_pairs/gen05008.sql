SELECT
    product_id,
    DATE_TRUNC('QUARTER', sale_date) AS quarter_start,
    CASE WHEN units >= 44 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    PERCENTILE_APPROX(units, 0.9) AS units_p90,
    PERCENTILE_APPROX(revenue, 0.5) AS revenue_p50,
    CORR(units, revenue) AS corr_units_revenue
FROM store_sales
GROUP BY product_id, quarter_start, units_tier;
