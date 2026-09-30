SELECT *
FROM (
    SELECT region, SUM(CASE WHEN city = 'Bangkok' THEN sqft ELSE 0 END) AS bangkok_sqft, COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count
    FROM stores
    GROUP BY region
) AS x
WHERE bangkok_sqft < 72
ORDER BY region ASC;
