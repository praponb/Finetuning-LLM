WITH ValidSuppliers AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('UK', 'US')
),
suppliers_stats AS (
    SELECT country, COUNT(DISTINCT rating) AS unique_citys
    FROM ValidSuppliers
    GROUP BY country
)
SELECT *
FROM suppliers_stats
ORDER BY unique_citys
