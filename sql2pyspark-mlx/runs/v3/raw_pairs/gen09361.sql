SELECT
    id,
    status,
    start_date,
    RANK() OVER (PARTITION BY status ORDER BY start_date DESC) AS rnk
FROM subscriptions
