SELECT *
FROM (
    SELECT
        id,
        plan,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) row_num
    FROM subscriptions
    WHERE MONTH(start_date) IN (1, 11, 12)
) ranked
WHERE row_num <= 5
