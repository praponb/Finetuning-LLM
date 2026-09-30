SELECT
    city,
    CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    STDDEV_SAMP(rating) AS rating_stddev,
    STDDEV_SAMP(rating) AS rating_stddev_2,
    AVG(rating) AS avg_rating,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS rating_per_singapore,
    COUNT(DISTINCT city) AS distinct_cities
FROM suppliers
GROUP BY city, CASE WHEN rating >= 4.2 THEN 'HIGH' ELSE 'LOW' END
ORDER BY rating_stddev_2
