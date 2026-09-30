SELECT
    id,
    customer_id,
    status,
    monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id, status), 3) AS monthly_fee_share
FROM subscriptions
