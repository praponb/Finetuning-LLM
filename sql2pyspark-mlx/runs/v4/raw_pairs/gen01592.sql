SELECT
    CASE WHEN revenue >= 3900 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_tier,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    ROUND(AVG(units), 2) AS avg_units,
    STDDEV_SAMP(units) AS units_stddev,
    CORR(units, revenue) AS corr_units_revenue,
    ROUND(AVG(units), 2) AS avg_units_2,
    ROUND(AVG(units), 2) AS avg_units_3
FROM store_sales
GROUP BY CUBE (CASE WHEN revenue >= 3900 THEN 'PREMIUM' ELSE 'STANDARD' END)
HAVING STDDEV_SAMP(units) >= 0
ORDER BY avg_units_2 DESC NULLS LAST
