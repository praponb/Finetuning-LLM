SELECT
    city,
    region,
    AVG(sqft) AS avg_sqft,
    SUM(CASE WHEN city = 'Chiang Mai' THEN sqft ELSE 0 END) AS chiang_mai_sqft
FROM stores
WHERE region <> 'West' AND city NOT IN ('Tokyo', 'Berlin', 'New York') AND sqft < 10082
GROUP BY city, region
HAVING AVG(sqft) < 16000
ORDER BY avg_sqft, chiang_mai_sqft DESC
