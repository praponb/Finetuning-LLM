SELECT id, plan, start_date
FROM (
    SELECT
        id,
        plan,
        start_date,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY start_date DESC) AS rn
    FROM subscriptions
    WHERE end_date > '2025-04-01'
) AS t
WHERE rn <= 5
ORDER BY start_date DESC
