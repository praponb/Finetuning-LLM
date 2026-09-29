WITH src AS (
    SELECT *, DATE_SUB(end_date, 9) AS end_date_minus_9d
    FROM subscriptions
    WHERE status IN ('EXPIRED', 'CANCELLED', 'ACTIVE') AND plan = 'standard'
),
subscriptions_stats AS (
    SELECT DATE_FORMAT(start_date, 'yyyy-MM') AS start_month, SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee
    FROM src
    GROUP BY DATE_FORMAT(start_date, 'yyyy-MM')
)
SELECT *
FROM subscriptions_stats
ORDER BY enterprise_monthly_fee;
