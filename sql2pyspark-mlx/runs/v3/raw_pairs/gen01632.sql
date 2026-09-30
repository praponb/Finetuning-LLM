WITH ranked AS (
    SELECT
        id,
        status,
        start_date,
        plan,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date, id) AS row_num
    FROM subscriptions
)
SELECT *
FROM ranked
WHERE row_num <= 3
ORDER BY plan;
