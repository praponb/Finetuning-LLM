SELECT
    id,
    customer_id,
    monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 3) AS monthly_fee_share
FROM subscriptions
ORDER BY monthly_fee DESC
