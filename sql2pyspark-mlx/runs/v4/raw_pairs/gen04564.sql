SELECT
    id,
    plan,
    monthly_fee,
    status,
    start_date,
    DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC) AS monthly_fee_dense_rank,
    SUM(monthly_fee) OVER (PARTITION BY status ORDER BY start_date, id) AS running_monthly_fee
FROM subscriptions
WHERE monthly_fee <= 110;
