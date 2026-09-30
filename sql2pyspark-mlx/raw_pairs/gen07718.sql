SELECT
    id,
    customer_id,
    plan,
    monthly_fee,
    AVG(monthly_fee) OVER (PARTITION BY customer_id, plan) AS customer_id_avg_monthly_fee
FROM subscriptions
ORDER BY id
