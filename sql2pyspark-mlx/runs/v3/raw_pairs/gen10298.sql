SELECT id, plan, start_date
FROM (
    SELECT
        id,
        plan,
        start_date,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY start_date DESC) AS rnk
    FROM subscriptions
) AS ranked
WHERE rnk <= 5;
