SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    plan,
    end_date,
    SUM(monthly_fee) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS running_monthly_fee,
    LEAD(monthly_fee, 1) OVER (PARTITION BY plan ORDER BY end_date, id) AS next_monthly_fee
FROM subscriptions
WHERE plan = 'standard';
