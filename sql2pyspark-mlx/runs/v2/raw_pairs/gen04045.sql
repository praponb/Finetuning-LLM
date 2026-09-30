WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country = 'DE'
),
agg AS (
    SELECT city, COUNT(DISTINCT rating) AS unique_countrys
    FROM filtered_suppliers
    GROUP BY city
)
SELECT *
FROM agg
WHERE unique_countrys >= 4
