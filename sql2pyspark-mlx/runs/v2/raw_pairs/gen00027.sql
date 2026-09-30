WITH src AS (
    SELECT *
    FROM suppliers
    WHERE country = 'US'
),
SuppliersStats AS (
    SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(DISTINCT rating) AS unique_countrys
    FROM src
    GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM SuppliersStats
WHERE unique_countrys < 6
