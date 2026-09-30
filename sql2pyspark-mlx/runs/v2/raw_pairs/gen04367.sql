WITH RankedSubscriptions AS (
    SELECT
        id,
        status,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date, id) pos
    FROM subscriptions
)
SELECT id, status, end_date
FROM RankedSubscriptions
WHERE pos <= 3
