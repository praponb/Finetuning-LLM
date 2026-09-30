WITH src AS (
    SELECT *, DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
    FROM subscriptions
    WHERE customer_id IN (27, 37, 38)
),
subscriptions_stats AS (
    SELECT
        status,
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_month,
        SUM(CASE WHEN plan = 'standard' THEN days_since_end_date ELSE 0 END) AS standard_days_since_end_date,
        MAX(start_date) AS last_start_date
    FROM src
    GROUP BY status, DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT *
FROM subscriptions_stats
ORDER BY standard_days_since_end_date DESC
