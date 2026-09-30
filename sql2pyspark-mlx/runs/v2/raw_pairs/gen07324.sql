WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE city = 'Singapore'
),
Agg AS (
    SELECT country, COUNT(rating) AS rating_count, MAX(rating) - MIN(rating) AS rating_range
    FROM filtered_suppliers
    GROUP BY country
)
SELECT *
FROM Agg
WHERE rating_count >= 4
ORDER BY 3;
