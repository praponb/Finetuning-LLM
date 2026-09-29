WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE segment <> 'Corporate' AND city = 'New York'
),
agg AS (
    SELECT segment, is_active, COUNT(CASE WHEN city = 'Berlin' THEN 1 END) berlin_count
    FROM filtered_customers
    GROUP BY segment, is_active
)
SELECT *
FROM agg
WHERE berlin_count > 10
ORDER BY segment, is_active
LIMIT 10
