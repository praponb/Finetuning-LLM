SELECT
    CASE WHEN sqft >= 14936 THEN 'PREMIUM' ELSE 'STANDARD' END AS sqft_tier,
    city,
    COUNT(DISTINCT city) AS distinct_cities,
    AVG(sqft) FILTER (WHERE city = 'New York') AS new_york_avg_sqft,
    COUNT_IF(city = 'Bangkok') AS bangkok_events,
    BIT_OR(sqft) AS sqft_bit_or,
    GROUPING_ID() AS grouping_id
FROM stores
GROUP BY GROUPING SETS ((CASE WHEN sqft >= 14936 THEN 'PREMIUM' ELSE 'STANDARD' END, city), (CASE WHEN sqft >= 14936 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING sqft_bit_or > 1;
