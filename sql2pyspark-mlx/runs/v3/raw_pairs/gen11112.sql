SELECT
    id,
    plan,
    status,
    end_date,
    monthly_fee,
    LEAD(monthly_fee, 2) OVER (PARTITION BY plan, status ORDER BY end_date, id) AS next_monthly_fee
FROM subscriptions
