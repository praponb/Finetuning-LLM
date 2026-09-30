SELECT city, age_range, th_count
FROM (
    SELECT city, MAX(age) - MIN(age) AS age_range, COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count
    FROM customers
    GROUP BY city
) AS s
WHERE th_count >= 3
ORDER BY th_count DESC
