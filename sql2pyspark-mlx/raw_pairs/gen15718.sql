SELECT
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    city,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'South' THEN 1 END), 0), 2) AS sqft_per_south,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    COUNT(CASE WHEN city = 'Chiang Mai' THEN 1 END) AS chiang_mai_count,
    COUNT(DISTINCT city) AS distinct_cities,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('QUARTER', opened_date) ORDER BY COUNT(DISTINCT city) DESC) AS distinct_cities_dense_rank
FROM stores
GROUP BY 1, 2, 3
ORDER BY city
