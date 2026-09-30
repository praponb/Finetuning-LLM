SELECT id, plan, end_date
FROM (
    SELECT
        id,
        plan,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS pos
    FROM subscriptions
    WHERE monthly_fee >= 150
) AS r
WHERE pos <= 3
ORDER BY end_date
