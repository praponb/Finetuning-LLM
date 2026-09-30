WITH filtered_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE status NOT IN ('EXPIRED', 'CANCELLED') AND (customer_id = 16 OR plan IN ('basic', 'premium', 'enterprise'))
),
summary AS (
    SELECT plan, status, COUNT(DISTINCT monthly_fee) AS unique_statuss
    FROM filtered_subscriptions
    GROUP BY plan, status
)
SELECT *
FROM summary
WHERE unique_statuss >= 10;
