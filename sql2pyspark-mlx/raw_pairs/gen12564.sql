WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, value
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(created_at) OVER (PARTITION BY customer_id ORDER BY created_at, id, attr_key) IS NULL
                 OR created_at > LAG(created_at) OVER (PARTITION BY customer_id ORDER BY created_at, id, attr_key) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM exploded
),
sessions_carts AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY customer_id ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM sessions_carts src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.created_at BETWEEN sub.start_date AND sub.end_date
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM attributed src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.created_at >= sub.start_date
       AND src.created_at < sub.end_date + INTERVAL 14 DAYS
)
-- Grouped metrics
SELECT
    CASE WHEN monthly_fee >= 230 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_tier,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    SUM(subscription_monthly_fee) AS total_subscription_monthly_fee,
    SUM(CASE WHEN subscription_plan = 'basic' THEN subscription_id ELSE 0 END) AS basic_subscription_id,
    APPROX_PERCENTILE(session_no, 0.9) AS session_no_p90
FROM matched
GROUP BY CASE WHEN monthly_fee >= 230 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', created_at)
ORDER BY monthly_fee_tier NULLS LAST
