SELECT
    id,
    status,
    end_date,
    monthly_fee,
    LAG(monthly_fee, 1) OVER (PARTITION BY status ORDER BY end_date, id) AS prev_monthly_fee
FROM subscriptions;
