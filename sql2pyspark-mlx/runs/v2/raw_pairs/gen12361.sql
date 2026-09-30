WITH top_subscriptions AS (
    SELECT
        id,
        status,
        monthly_fee,
        plan,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY monthly_fee DESC, id) AS rn
    FROM subscriptions
)
SELECT *
FROM top_subscriptions
WHERE rn <= 2
ORDER BY id DESC, status
