SELECT
    CASE WHEN rating >= 4.0 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    PERCENTILE_APPROX(rating, 0.95) AS rating_p95
FROM suppliers
GROUP BY rating_tier
HAVING rating_p95 < 1000000
ORDER BY rating_p95 NULLS FIRST;
