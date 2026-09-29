SELECT
    id,
    customer_id,
    start_date,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY start_date) AS dense_rnk
FROM subscriptions
WHERE customer_id IN (12, 20, 22, 25);
