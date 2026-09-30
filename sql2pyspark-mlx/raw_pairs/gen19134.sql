WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM sessions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at BETWEEN sub.start_date AND sub.end_date
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id_2,
        sub.plan AS subscription_plan,
        sub.monthly_fee AS subscription_monthly_fee
    FROM with_window a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.started_at >= sub.start_date
       AND a.started_at < sub.end_date + INTERVAL 30 DAYS
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.device,
        s.started_at,
        s.subscription_id,
        s.plan,
        s.subscription_id_2,
        s.subscription_plan,
        s.subscription_monthly_fee,
        latest.event_time
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN subscription_id >= 9 THEN 'High' WHEN subscription_id >= 5 THEN 'Medium' ELSE 'Low' END AS subscription_id_level,
        COALESCE(subscription_plan, 'OTHER') AS resolved_subscription_plan
    FROM with_last
    WHERE started_at > TO_TIMESTAMP('2023-05-07 12:00:00')
      AND plan IS NOT NULL
),
base_sessions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(subscription_id_2, 0) AS subscription_id_2_filled
    FROM base
    WHERE plan IS NOT NULL
      AND subscription_plan NOT IN ('standard', 'basic')
)
-- Grouped metrics
SELECT
    CASE WHEN subscription_monthly_fee >= 250 THEN 'PREMIUM' ELSE 'STANDARD' END AS subscription_monthly_fee_tier,
    COUNT(DISTINCT subscription_plan) AS distinct_subscription_plans,
    MIN(started_at) AS first_started_at,
    MIN(event_time) AS first_event_time,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count
FROM base_sessions
GROUP BY CASE WHEN subscription_monthly_fee >= 250 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY distinct_subscription_plans DESC NULLS FIRST
