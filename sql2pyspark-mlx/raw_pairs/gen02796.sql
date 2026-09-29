WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country != 'TH'
),
CountrySummary AS (
    SELECT country, MAX(rating) AS max_rating, ROUND(AVG(rating), 2) AS avg_rating
    FROM recent_suppliers
    GROUP BY country
)
SELECT country, max_rating, avg_rating
FROM CountrySummary
ORDER BY avg_rating DESC;
