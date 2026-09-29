SELECT
    id,
    customer_id,
    monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 3) monthly_fee_share
FROM subscriptions
ORDER BY customer_id DESC
