WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('JP', 'DE', 'US')
),
city_summary AS (
    SELECT city, COUNT(rating) AS rating_count
    FROM recent_suppliers
    GROUP BY city
)
SELECT *
FROM city_summary
WHERE rating_count < 10;
