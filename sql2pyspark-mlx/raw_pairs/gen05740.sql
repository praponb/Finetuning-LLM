WITH filtered_suppliers AS (
    SELECT *, TRIM(city) AS clean_city
    FROM suppliers
    WHERE country IN ('JP', 'UK', 'TH') AND rating > 3.4
),
country_summary AS (
    SELECT country, ROUND(AVG(rating), 2) AS avg_rating, COUNT(*) AS total_count
    FROM filtered_suppliers
    GROUP BY country
)
SELECT country, total_count
FROM country_summary
WHERE total_count < 7;
