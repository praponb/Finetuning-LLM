SELECT
    id,
    plan,
    monthly_fee,
    end_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS pos
    FROM subscriptions
    WHERE MONTH(start_date) IN (1, 5, 11)
) AS r
WHERE pos = 1
ORDER BY id DESC, plan
