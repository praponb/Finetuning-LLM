SELECT
    id,
    start_date,
    monthly_fee,
    customer_id
FROM subscriptions
WHERE start_date = (SELECT MIN(start_date) FROM subscriptions WHERE MONTH(start_date) IN (6, 7, 12));
