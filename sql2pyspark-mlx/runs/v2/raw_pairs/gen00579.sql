WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE YEAR(end_date) = 2024 AND status <> 'ACTIVE' AND status = 'CANCELLED'
),
subscriptions_stats AS (
    SELECT status, SUM(CASE WHEN status = 'EXPIRED' THEN monthly_fee ELSE 0 END) AS expired_monthly_fee
    FROM recent_subscriptions
    GROUP BY status
)
SELECT status, expired_monthly_fee
FROM subscriptions_stats
