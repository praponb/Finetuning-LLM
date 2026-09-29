SELECT
    id,
    plan,
    start_date,
    monthly_fee,
    ROUND(AVG(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) monthly_fee_moving_avg_7
FROM subscriptions;
