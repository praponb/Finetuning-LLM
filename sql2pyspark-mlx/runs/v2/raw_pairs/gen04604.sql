SELECT country, corporate_count, min_age
FROM (
    SELECT country, COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count, MIN(age) AS min_age
    FROM customers
    GROUP BY country
) AS x
WHERE corporate_count >= 5
