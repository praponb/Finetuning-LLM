WITH src AS (
    SELECT *, COALESCE(monthly_fee, 0) AS monthly_fee_filled
    FROM subscriptions
    WHERE monthly_fee >= 230
),
subscriptions_stats AS (
    SELECT status, SUM(monthly_fee_filled) AS total_monthly_fee_filled
    FROM src
    GROUP BY status
)
SELECT status, total_monthly_fee_filled
FROM subscriptions_stats
ORDER BY status DESC
LIMIT 1;
