SELECT
    id,
    status,
    monthly_fee,
    plan,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 3) AS monthly_fee_share,
    AVG(monthly_fee) OVER (PARTITION BY plan) AS plan_avg_monthly_fee
FROM subscriptions
WHERE status != 'ACTIVE'
