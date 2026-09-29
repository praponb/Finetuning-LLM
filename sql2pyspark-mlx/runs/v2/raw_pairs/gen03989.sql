WITH valid_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE start_date < '2024-09-14'
),
summary AS (
    SELECT status, DATE_FORMAT(end_date, 'yyyy-MM') AS end_month, SUM(CASE WHEN plan = 'standard' THEN 1 ELSE 0 END) AS standard_count
    FROM valid_subscriptions
    GROUP BY status, DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT *
FROM summary
WHERE standard_count >= 75
ORDER BY end_month, status
LIMIT 3
