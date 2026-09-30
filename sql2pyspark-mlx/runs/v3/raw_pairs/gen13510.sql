SELECT
    id,
    plan,
    start_date,
    RANK() OVER (PARTITION BY plan ORDER BY start_date) AS start_date_rank
FROM subscriptions
WHERE status NOT IN ('PAUSED', 'EXPIRED')
ORDER BY id DESC
