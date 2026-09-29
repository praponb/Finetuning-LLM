SELECT
    start_date,
    plan,
    customer_id,
    CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END AS is_cancelled
FROM subscriptions
WHERE MONTH(end_date) IN (5, 9, 11);
