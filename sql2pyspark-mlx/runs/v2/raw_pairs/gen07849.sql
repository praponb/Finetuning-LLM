SELECT region, city, row_count
FROM (
    SELECT region, city, COUNT(*) AS row_count
    FROM stores
    GROUP BY region, city
) AS s
WHERE row_count >= 8
ORDER BY city DESC;
