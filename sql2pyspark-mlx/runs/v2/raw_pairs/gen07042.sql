WITH src AS (
    SELECT *
    FROM suppliers
    WHERE country = 'DE'
),
agg AS (
    SELECT city, MAX(rating) AS max_rating, COUNT(*) AS total_count
    FROM src
    GROUP BY city
)
SELECT city, max_rating, total_count
FROM agg
ORDER BY max_rating DESC
