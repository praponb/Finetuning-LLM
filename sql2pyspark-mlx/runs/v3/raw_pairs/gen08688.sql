SELECT
    id,
    plan,
    status,
    monthly_fee,
    MAX(monthly_fee) OVER (PARTITION BY plan, status) AS plan_max_monthly_fee
FROM subscriptions
WHERE status = 'CANCELLED'
ORDER BY plan DESC, plan_max_monthly_fee DESC;
