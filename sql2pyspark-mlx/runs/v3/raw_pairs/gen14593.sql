SELECT
    id,
    status,
    customer_id,
    start_date,
    LAST_DAY(start_date) AS start_date_month_end,
    TRIM(status) AS clean_status
FROM subscriptions
WHERE NOT (customer_id IN (3, 19, 29) AND status = 'PAUSED' AND end_date < '2023-03-01')
ORDER BY start_date, id
LIMIT 5
