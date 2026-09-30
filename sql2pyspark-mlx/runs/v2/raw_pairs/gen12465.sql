WITH valid_suppliers AS (
    SELECT *, CASE WHEN country IN ('US', 'JP') THEN 'Group A' ELSE 'Other' END AS country_group
    FROM suppliers
    WHERE supplier_name LIKE '%Sia%'
),
country_summary AS (
    SELECT country, ROUND((100 * SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_de
    FROM valid_suppliers
    GROUP BY country
)
SELECT country, pct_de
FROM country_summary
WHERE pct_de > 67
