WITH recent_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE monthly_fee >= 220
),
status_summary AS (
    SELECT status, COUNT(monthly_fee) AS monthly_fee_count, COUNT(DISTINCT monthly_fee) AS unique_statuss
    FROM recent_subscriptions
    GROUP BY status
)
SELECT status, monthly_fee_count, unique_statuss
FROM status_summary
WHERE monthly_fee_count > 6;
