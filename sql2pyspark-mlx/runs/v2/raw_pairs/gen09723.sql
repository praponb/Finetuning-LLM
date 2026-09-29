WITH top_subscriptions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS pos
    FROM subscriptions
    WHERE customer_id IS NOT NULL
)
SELECT *
FROM top_subscriptions
WHERE pos <= 2
ORDER BY monthly_fee ASC
