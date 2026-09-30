SELECT
    id,
    status,
    start_date,
    customer_id,
    DATE_FORMAT(end_date, 'MMM yyyy') AS end_date_label
FROM subscriptions
WHERE NOT (status = 'EXPIRED' OR customer_id IN (16, 17, 38))
ORDER BY customer_id DESC
