SELECT id, status, monthly_fee, ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY status), 3) AS monthly_fee_share FROM subscriptions
