WITH Src AS (
    SELECT *
    FROM suppliers
    WHERE rating <= 2.4
),
summary AS (
    SELECT city, MIN(rating) AS min_rating, ROUND((100 * SUM(CASE WHEN country = 'JP' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_jp
    FROM Src
    GROUP BY city
)
SELECT city, min_rating, pct_jp
FROM summary
WHERE min_rating >= 3.5
ORDER BY pct_jp DESC;
