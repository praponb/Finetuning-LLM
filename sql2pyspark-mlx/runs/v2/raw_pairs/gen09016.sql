SELECT
    id,
    start_date,
    status,
    DATE_TRUNC('WEEK', start_date) AS start_date_period
FROM subscriptions
WHERE NOT (customer_id = 39 AND plan = 'premium' AND end_date >= '2024-05-01')
ORDER BY start_date, id
LIMIT 10;
