WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC NULLS LAST, id ASC) = 1
)
SELECT
    id,
    customer_id,
    plan,
    monthly_fee,
    status
FROM ranked
ORDER BY status DESC
