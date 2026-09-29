SELECT *
FROM (
    SELECT
        id,
        plan,
        end_date,
        customer_id,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY end_date DESC) pos
    FROM subscriptions
) t
WHERE pos <= 2
