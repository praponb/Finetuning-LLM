SELECT
    CASE WHEN units >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END AS units_tier,
    CASE WHEN revenue >= 3500 THEN 'HIGH' ELSE 'LOW' END AS revenue_tier,
    PERCENTILE_APPROX(revenue, 0.5) AS revenue_p50,
    PERCENTILE_APPROX(units, ARRAY(0.25, 0.5, 0.75)) AS units_quartiles,
    ROUND(AVG(revenue), 2) AS avg_revenue
FROM store_sales
GROUP BY CASE WHEN units >= 35 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN revenue >= 3500 THEN 'HIGH' ELSE 'LOW' END
HAVING avg_revenue >= 0
ORDER BY revenue_p50 DESC NULLS LAST;
