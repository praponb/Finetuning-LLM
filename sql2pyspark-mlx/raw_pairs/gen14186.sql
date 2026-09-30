SELECT
    CASE WHEN units >= 32 THEN 'HIGH' ELSE 'LOW' END AS units_tier,
    BIT_AND(units) AS units_bit_and,
    APPROX_PERCENTILE(revenue, ARRAY(0.25, 0.5, 0.75)) AS revenue_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    STDDEV_SAMP(revenue) AS revenue_stddev,
    STDDEV_SAMP(revenue) AS revenue_stddev_2
FROM store_sales
GROUP BY CASE WHEN units >= 32 THEN 'HIGH' ELSE 'LOW' END
ORDER BY units_bit_and DESC
