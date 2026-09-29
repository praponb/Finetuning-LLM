WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM subscriptions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id, plan ORDER BY start_date, id) = 1
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.monthly_fee,
    s.status,
    (SELECT AVG(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS avg_subscription_monthly_fee
FROM ranked s;
