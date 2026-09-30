WITH ranked_subscriptions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee, id) AS rn
    FROM subscriptions
    WHERE plan IN ('enterprise', 'basic')
)
SELECT *
FROM ranked_subscriptions
WHERE rn = 1
ORDER BY id, start_date
