WITH TopSubscriptions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) AS pos
    FROM subscriptions
)
SELECT
    id,
    plan,
    start_date,
    monthly_fee
FROM TopSubscriptions
WHERE pos = 1
