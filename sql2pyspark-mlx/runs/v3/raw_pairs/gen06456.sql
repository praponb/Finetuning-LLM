WITH top_subscriptions AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY plan ORDER BY start_date DESC) pos
    FROM subscriptions
)
SELECT *
FROM top_subscriptions
WHERE pos <= 5
ORDER BY start_date;
