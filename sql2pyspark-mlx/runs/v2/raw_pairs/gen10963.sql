SELECT *
FROM (
    SELECT city, ROUND((100 * SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_chiang_mai, ROUND(STDDEV(sqft), 2) AS stddev_sqft
    FROM stores
    GROUP BY city
) AS t
WHERE pct_chiang_mai > 50
ORDER BY stddev_sqft DESC, pct_chiang_mai DESC
