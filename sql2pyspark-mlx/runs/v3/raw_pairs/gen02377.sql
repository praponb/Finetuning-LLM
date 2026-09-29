WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS rnk
    FROM subscriptions
)
SELECT
    id,
    plan,
    start_date,
    monthly_fee
FROM ranked
WHERE rnk <= 3
ORDER BY start_date ASC
