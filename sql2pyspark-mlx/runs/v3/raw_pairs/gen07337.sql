SELECT
    id,
    plan,
    end_date,
    monthly_fee,
    LAG(monthly_fee, 2) OVER (PARTITION BY plan ORDER BY end_date, id) AS prev_monthly_fee
FROM subscriptions;
