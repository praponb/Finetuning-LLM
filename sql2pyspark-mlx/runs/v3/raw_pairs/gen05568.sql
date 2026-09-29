SELECT *
FROM (
    SELECT
        id,
        status,
        end_date,
        start_date,
        RANK() OVER (PARTITION BY status ORDER BY end_date DESC) AS rn
    FROM subscriptions
) AS t
WHERE rn <= 2
ORDER BY start_date ASC, id ASC;
