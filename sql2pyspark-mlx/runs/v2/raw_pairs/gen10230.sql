WITH valid_suppliers AS (
    SELECT *, SUBSTRING(supplier_name, 1, 2) AS supplier_name_prefix
    FROM suppliers
    WHERE city NOT IN ('Bangkok', 'London') AND country IN ('TH', 'SG', 'DE')
),
summary AS (
    SELECT country, SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating
    FROM valid_suppliers
    GROUP BY country
)
SELECT country, jp_rating
FROM summary
WHERE jp_rating >= 40
ORDER BY country
LIMIT 10
