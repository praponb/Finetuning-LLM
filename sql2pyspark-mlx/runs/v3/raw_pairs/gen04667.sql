SELECT
    id,
    customer_id,
    start_date,
    DATEDIFF(CURRENT_DATE(), end_date) AS days_since_end_date
FROM subscriptions
WHERE status = 'ACTIVE' AND start_date < '2025-05-06';
