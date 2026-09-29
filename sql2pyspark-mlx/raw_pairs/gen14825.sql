SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    LAG(monthly_fee, 1, 0) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS prev_monthly_fee
FROM subscriptions
