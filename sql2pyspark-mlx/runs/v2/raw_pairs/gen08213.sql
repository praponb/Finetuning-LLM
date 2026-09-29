SELECT
    id,
    plan,
    monthly_fee,
    start_date,
    RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC) AS rnk,
    LAG(monthly_fee, 1, 0) OVER (ORDER BY start_date, id) AS prev_monthly_fee
FROM subscriptions;
