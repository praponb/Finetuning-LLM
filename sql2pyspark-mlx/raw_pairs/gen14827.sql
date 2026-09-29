SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
FROM subscriptions
WHERE MONTH(end_date) IN (7, 9, 11)
