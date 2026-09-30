SELECT
    id,
    customer_id,
    start_date,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date, id) start_date_row
FROM subscriptions;
