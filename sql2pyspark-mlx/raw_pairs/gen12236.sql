SELECT *
FROM (
    SELECT
        id,
        plan,
        end_date,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS row_num
    FROM subscriptions
) AS t
WHERE row_num = 1;
