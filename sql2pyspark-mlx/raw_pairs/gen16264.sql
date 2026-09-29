SELECT
    id,
    plan,
    start_date,
    monthly_fee
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date DESC, id) AS row_num
    FROM subscriptions
) AS r
WHERE row_num <= 5
ORDER BY id DESC, start_date
