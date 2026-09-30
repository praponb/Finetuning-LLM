WITH src AS (
    SELECT *, LENGTH(city) AS city_length
    FROM stores
    WHERE opened_date < '2025-07-23'
),
stores_stats AS (
    SELECT city, ROUND(AVG(city_length), 2) AS avg_city_length, COUNT(city_length) AS city_length_count
    FROM src
    GROUP BY city
)
SELECT city, avg_city_length, city_length_count
FROM stores_stats
WHERE avg_city_length >= 47
ORDER BY city
LIMIT 10
