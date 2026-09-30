SELECT
    id,
    customer_id,
    end_date,
    monthly_fee,
    LAG(monthly_fee, 1) OVER (PARTITION BY customer_id ORDER BY end_date, id) AS prev_monthly_fee
FROM subscriptions
WHERE status IN ('ACTIVE', 'EXPIRED', 'CANCELLED');
