SELECT
    id,
    status,
    monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY status) AS status_sum_monthly_fee
FROM subscriptions
