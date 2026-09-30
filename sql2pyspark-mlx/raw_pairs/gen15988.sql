WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country = 'JP' AND country NOT IN ('DE', 'JP')
),
country_group_summary AS (
    SELECT CASE WHEN country IN ('JP', 'SG') THEN 'tier_1' ELSE 'other' END AS country_group, COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count
    FROM recent_suppliers
    GROUP BY CASE WHEN country IN ('JP', 'SG') THEN 'tier_1' ELSE 'other' END
)
SELECT country_group, singapore_count
FROM country_group_summary
WHERE singapore_count >= 5
ORDER BY singapore_count DESC
