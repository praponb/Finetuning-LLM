SELECT
    id,
    customer_id,
    plan,
    start_date,
    RANK() OVER (PARTITION BY customer_id, plan ORDER BY start_date) AS rnk
FROM subscriptions
ORDER BY id DESC
