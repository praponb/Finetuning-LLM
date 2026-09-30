SELECT opened_date, region
FROM stores
WHERE region NOT IN ('West', 'Central')
UNION
SELECT opened_date, region
FROM stores
WHERE region IS NOT NULL
ORDER BY region
