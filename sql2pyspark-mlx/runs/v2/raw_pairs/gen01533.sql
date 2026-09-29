WITH valid_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name NOT LIKE '%Wholesale%' AND rating BETWEEN 2.0 AND 2.9 AND country = 'SG'
),
suppliers_stats AS (
    SELECT country, SUM(CASE WHEN city = 'Chiang Mai' THEN rating ELSE 0 END) AS chiang_mai_rating
    FROM valid_suppliers
    GROUP BY country
)
SELECT country, chiang_mai_rating
FROM suppliers_stats
WHERE chiang_mai_rating >= 73
ORDER BY 1 ASC
