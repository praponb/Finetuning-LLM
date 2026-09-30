SELECT
    CASE WHEN rating >= 4.4 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    COUNT(*) FILTER (WHERE city = 'Bangkok') AS bangkok_rows,
    SIZE(COLLECT_SET(country)) AS num_countries,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Bangkok' THEN 1 END), 0), 2) AS rating_per_bangkok,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS rating_per_de
FROM suppliers
GROUP BY CASE WHEN rating >= 4.4 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING num_countries < 1;
