SELECT *
FROM (
    SELECT city, MIN(sqft) AS min_sqft, COUNT(*) AS row_count
    FROM stores
    GROUP BY city
) AS t
WHERE min_sqft >= 9900
ORDER BY row_count DESC;
