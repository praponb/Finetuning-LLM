WITH valid_customers AS (
    SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label
    FROM customers
    WHERE email NOT LIKE '%@startup.io'
),
Agg AS (
    SELECT is_active, COUNT(DISTINCT age) AS unique_countrys, ROUND((100 * SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_consumer
    FROM valid_customers
    GROUP BY is_active
)
SELECT is_active, unique_countrys, pct_consumer
FROM Agg
WHERE pct_consumer < 72
ORDER BY unique_countrys DESC;
