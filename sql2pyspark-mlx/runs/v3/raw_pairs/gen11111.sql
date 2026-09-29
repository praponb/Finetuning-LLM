SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS running_monthly_fee
FROM subscriptions;
