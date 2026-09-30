SELECT
    is_active,
    city,
    segment,
    MAX(age) - MIN(age) AS age_range
FROM customers
WHERE city = 'London'
GROUP BY is_active, city, segment
ORDER BY age_range;
