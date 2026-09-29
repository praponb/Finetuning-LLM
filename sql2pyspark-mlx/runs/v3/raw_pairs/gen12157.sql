SELECT
    id,
    customer_id,
    start_date,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY start_date DESC) AS dense_rnk
FROM subscriptions
