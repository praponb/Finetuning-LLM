WITH Base AS (
    SELECT *
    FROM suppliers
    WHERE NOT (country != 'SG' AND rating < 2.9)
),
summary AS (
    SELECT city, AVG(rating) AS avg_rating
    FROM Base
    GROUP BY city
)
SELECT city, avg_rating
FROM summary
WHERE avg_rating >= 3.1
ORDER BY avg_rating DESC
