SELECT country, chiang_mai_count
FROM (
    SELECT country, SUM(CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END) AS chiang_mai_count
    FROM suppliers
    GROUP BY country
) AS s
WHERE chiang_mai_count < 40;
