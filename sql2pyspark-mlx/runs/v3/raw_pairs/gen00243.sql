SELECT country, th_count
FROM (
    SELECT country, COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count
    FROM suppliers
    WHERE city = 'Berlin'
    GROUP BY country
) AS s
WHERE th_count > 6;
