WITH valid_customers AS (
    SELECT *
    FROM customers
    WHERE NOT is_active
),
customers_stats AS (
    SELECT segment, SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END) AS de_count, COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count
    FROM valid_customers
    GROUP BY segment
)
SELECT *
FROM customers_stats
WHERE bangkok_count < 4
ORDER BY 1;
