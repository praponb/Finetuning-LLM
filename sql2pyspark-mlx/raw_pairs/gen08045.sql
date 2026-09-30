SELECT
    id,
    customer_id,
    end_date,
    monthly_fee,
    COUNT(*) OVER (PARTITION BY customer_id) AS customer_id_count,
    LEAD(monthly_fee, 2) OVER (PARTITION BY customer_id ORDER BY end_date, id) AS next_monthly_fee
FROM subscriptions
WHERE monthly_fee < 130
