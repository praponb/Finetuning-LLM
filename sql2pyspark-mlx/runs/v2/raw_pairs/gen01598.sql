WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('US', 'TH', 'UK')
),
country_summary AS (
    SELECT country, MAX(rating) - MIN(rating) AS rating_range
    FROM valid_suppliers
    GROUP BY country
)
SELECT country, rating_range
FROM country_summary
ORDER BY rating_range, country
LIMIT 10
