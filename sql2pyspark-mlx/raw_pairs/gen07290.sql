WITH remaining_keys AS (
    SELECT id, plan
    FROM subscriptions
    EXCEPT ALL
    SELECT id, plan
    FROM subscriptions
    WHERE plan = 'enterprise'
),
kept_rows AS (
    SELECT s.*
    FROM subscriptions s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.plan = k.plan
)
SELECT
    id,
    plan,
    end_date,
    monthly_fee
FROM kept_rows
ORDER BY end_date DESC NULLS LAST
