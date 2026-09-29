SELECT
    CASE WHEN rating >= 3.9 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    STDDEV_SAMP(rating) AS rating_stddev,
    SUM(CASE WHEN city = 'Bangkok' THEN rating ELSE 0 END) AS bangkok_rating
FROM suppliers
GROUP BY 1
ORDER BY bangkok_rating DESC NULLS LAST;
