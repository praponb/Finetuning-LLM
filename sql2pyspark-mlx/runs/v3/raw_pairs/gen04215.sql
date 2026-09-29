SELECT
    status,
    plan,
    MAX(start_date) AS latest_start_date,
    ROUND(AVG(monthly_fee), 2) AS avg_monthly_fee
FROM subscriptions
GROUP BY status, plan
HAVING ROUND(AVG(monthly_fee), 2) >= 21
