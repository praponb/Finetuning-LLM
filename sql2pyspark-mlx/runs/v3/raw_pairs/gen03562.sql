SELECT segment, min_age
FROM (
    SELECT segment, MIN(age) AS min_age
    FROM customers
    GROUP BY segment
) AS x
WHERE min_age < 44
ORDER BY min_age ASC, segment ASC
LIMIT 50;
