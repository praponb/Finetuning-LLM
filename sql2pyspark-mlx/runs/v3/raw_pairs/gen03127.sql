SELECT
    plan,
    customer_id,
    MIN(start_date) AS first_start_date,
    ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee
FROM subscriptions
WHERE monthly_fee > 180
GROUP BY plan, customer_id
