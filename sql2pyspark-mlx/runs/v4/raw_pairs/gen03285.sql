SELECT
    CASE WHEN rating >= 3.8 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    COUNT(DISTINCT city) AS distinct_cities,
    AVG(rating) FILTER (WHERE city = 'Singapore') AS singapore_avg_rating,
    SUM(rating) FILTER (WHERE country = 'JP') AS jp_total_rating,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count
FROM suppliers
GROUP BY CASE WHEN rating >= 3.8 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY singapore_avg_rating NULLS LAST;
