SELECT
    id,
    customer_id,
    start_date,
    monthly_fee,
    LEAD(monthly_fee, 1, 0) OVER (PARTITION BY customer_id ORDER BY start_date, id) AS next_monthly_fee,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS row_num
FROM subscriptions;
