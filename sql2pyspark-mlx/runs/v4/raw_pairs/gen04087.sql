SELECT
    id,
    customer_id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    status,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS event_seq,
    SUM(monthly_fee) OVER (PARTITION BY plan ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee
FROM subscriptions;
