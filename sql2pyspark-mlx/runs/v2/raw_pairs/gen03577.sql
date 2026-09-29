WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('DE', 'SG', 'TH')
),
city_summary AS (
    SELECT city, COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count
    FROM filtered_suppliers
    GROUP BY city
)
SELECT *
FROM city_summary
WHERE singapore_count > 4
ORDER BY singapore_count
