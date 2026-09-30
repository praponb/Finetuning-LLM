SELECT
    id,
    customer_id,
    plan,
    start_date,
    monthly_fee,
    AVG(monthly_fee) OVER (ORDER BY start_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_5,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date, id) AS row_seq
FROM subscriptions
WHERE monthly_fee < 120
