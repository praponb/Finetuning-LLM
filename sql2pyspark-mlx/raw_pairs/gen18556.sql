SELECT
    CASE WHEN rating >= 4.5 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    APPROX_PERCENTILE(rating, 0.5) AS rating_p50,
    SIZE(COLLECT_SET(city)) AS num_cities,
    COUNT(DISTINCT id) AS distinct_ids
FROM suppliers
GROUP BY 1
ORDER BY rating_tier;
