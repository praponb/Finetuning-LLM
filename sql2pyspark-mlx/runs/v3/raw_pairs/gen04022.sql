WITH src AS (
    SELECT *
    FROM suppliers
    WHERE supplier_name IS NOT NULL AND supplier_name NOT LIKE 'Tok%' AND country IN ('UK', 'SG', 'JP')
),
summary AS (
    SELECT CASE WHEN city IN ('London', 'Singapore') THEN 'tier_1' ELSE 'other' END AS city_group, SUM(rating) AS total_rating
    FROM src
    GROUP BY CASE WHEN city IN ('London', 'Singapore') THEN 'tier_1' ELSE 'other' END
)
SELECT *
FROM summary
WHERE total_rating < 8
