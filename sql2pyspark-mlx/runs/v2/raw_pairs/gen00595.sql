SELECT
    id,
    customer_id,
    monthly_fee,
    plan,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 3) AS monthly_fee_share,
    MIN(monthly_fee) OVER (PARTITION BY plan) AS plan_min_monthly_fee
FROM subscriptions
ORDER BY customer_id DESC;
