WITH ranked AS (
    SELECT *, RANK() OVER (PARTITION BY status, plan ORDER BY end_date DESC NULLS LAST) AS rank_in_group
    FROM subscriptions
    QUALIFY rank_in_group = 1
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    status
FROM ranked;
