SELECT
    id,
    plan,
    end_date,
    monthly_fee,
    SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_monthly_fee
FROM subscriptions;
