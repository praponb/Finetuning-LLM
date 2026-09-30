SELECT *
FROM (
    SELECT
        id,
        plan,
        monthly_fee,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS pos
    FROM subscriptions
    WHERE plan = 'enterprise'
) AS t
WHERE pos = 1;
