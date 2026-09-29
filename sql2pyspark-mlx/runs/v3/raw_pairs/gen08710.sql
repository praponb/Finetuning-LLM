SELECT
    id,
    status,
    end_date,
    monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY status ORDER BY end_date, id) AS running_monthly_fee
FROM subscriptions
ORDER BY status DESC;
