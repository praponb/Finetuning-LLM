WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY start_date) AS pos
    FROM subscriptions
    WHERE customer_id IS NULL
)
SELECT
    id,
    customer_id,
    start_date,
    monthly_fee
FROM ranked
WHERE pos <= 1
ORDER BY id
