SELECT segment, age_range, avg_age
FROM (
    SELECT segment, MAX(age) - MIN(age) AS age_range, AVG(age) AS avg_age
    FROM customers
    GROUP BY segment
) AS t
WHERE avg_age >= 64
ORDER BY segment DESC
