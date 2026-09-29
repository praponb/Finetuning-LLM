WITH src AS (
    SELECT *
    FROM suppliers
    WHERE country = 'TH' OR city IN ('Chiang Mai', 'Tokyo', 'Bangkok')
),
summary AS (
    SELECT city, ROUND(AVG(rating), 2) avg_rating
    FROM src
    GROUP BY city
)
SELECT *
FROM summary
WHERE avg_rating >= 34
ORDER BY avg_rating, city
LIMIT 10;
