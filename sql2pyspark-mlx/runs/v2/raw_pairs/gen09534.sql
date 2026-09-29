WITH top_subscriptions AS (
    SELECT
        id,
        customer_id,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS pos
    FROM subscriptions
    WHERE start_date > '2025-06-11'
)
SELECT id, customer_id, start_date
FROM top_subscriptions
WHERE pos <= 3
ORDER BY start_date;
