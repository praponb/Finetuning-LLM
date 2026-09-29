WITH recent_suppliers AS (
    SELECT *, ROUND(rating, 0) AS rating_rounded
    FROM suppliers
    WHERE rating IS NOT NULL AND country != 'JP'
),
CitySummary AS (
    SELECT city, COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count
    FROM recent_suppliers
    GROUP BY city
)
SELECT city, bangkok_count
FROM CitySummary
ORDER BY bangkok_count
