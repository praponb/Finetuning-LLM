WITH top_subscriptions AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY end_date DESC, id) AS rn
    FROM subscriptions
)
SELECT
    id,
    plan,
    end_date,
    start_date
FROM top_subscriptions
WHERE rn <= 3;
