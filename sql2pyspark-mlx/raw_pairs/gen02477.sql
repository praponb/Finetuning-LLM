SELECT id, plan, start_date
FROM (
    SELECT
        id,
        plan,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS row_num
    FROM subscriptions
) AS r
WHERE row_num <= 3;
