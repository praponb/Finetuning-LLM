WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS pos
    FROM subscriptions
    WHERE plan = 'premium'
)
SELECT
    id,
    plan,
    start_date,
    customer_id
FROM ranked
WHERE pos <= 2;
