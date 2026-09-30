WITH valid_subscriptions AS (
    SELECT *
    FROM subscriptions
    WHERE start_date >= '2023-05-01'
),
subscriptions_stats AS (
    SELECT customer_id, COUNT(monthly_fee) AS monthly_fee_count
    FROM valid_subscriptions
    GROUP BY customer_id
)
SELECT *
FROM subscriptions_stats
ORDER BY customer_id DESC
LIMIT 5
