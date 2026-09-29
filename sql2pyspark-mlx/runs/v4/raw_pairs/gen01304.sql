SELECT
    id,
    status,
    start_date,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date, id) rn
FROM subscriptions
WHERE customer_id = 27;
