WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE rating > 3.5
),
country_summary AS (
    SELECT country, SUM(rating) AS total_rating, ROUND(AVG(rating), 2) AS avg_rating
    FROM valid_suppliers
    GROUP BY country
)
SELECT country, avg_rating
FROM country_summary
WHERE avg_rating > 74;
