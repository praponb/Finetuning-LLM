SELECT
    id,
    status,
    start_date,
    monthly_fee,
    plan,
    customer_id,
    end_date,
    monthly_fee - LAG(monthly_fee, 1) OVER (PARTITION BY status ORDER BY start_date, id) AS monthly_fee_change,
    LAG(monthly_fee, 1) OVER (PARTITION BY plan, customer_id ORDER BY end_date, id) AS prev_monthly_fee
FROM subscriptions
