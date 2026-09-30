WITH numbered AS (
    SELECT
        id,
        plan,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS row_num
    FROM subscriptions
    WHERE plan IN ('premium', 'standard')
)
SELECT *
FROM numbered
WHERE row_num = 1
ORDER BY plan;
