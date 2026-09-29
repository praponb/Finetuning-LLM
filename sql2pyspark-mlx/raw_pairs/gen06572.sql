WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE monthly_fee <= 130
),
summary AS (
    SELECT status, SUM(CASE WHEN plan = 'enterprise' THEN monthly_fee ELSE 0 END) AS enterprise_monthly_fee, ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
    FROM src
    GROUP BY status
)
SELECT status, enterprise_monthly_fee
FROM summary
WHERE enterprise_monthly_fee < 49
ORDER BY enterprise_monthly_fee DESC
