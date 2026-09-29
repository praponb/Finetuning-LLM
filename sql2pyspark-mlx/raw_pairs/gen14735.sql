WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        DATE_FORMAT(started_at, 'yyyy-MM') started_at_label,
        COALESCE(browser, 'DIRECT') resolved_browser
    FROM sessions
    WHERE started_at > DATE '2023-09-24'
      AND customer_id IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('week', started_at), device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country_2
    FROM base
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time < '2025-03-01'
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.device,
        e.started_at,
        e.country,
        e.started_at_label,
        e.resolved_browser,
        e.first_known_country_2,
        d.country AS customer_country
    FROM kept e
    INNER JOIN customers d
        ON e.customer_id <=> d.id
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM enriched src
    INNER JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.started_at >= sub.start_date
       AND src.started_at <= DATE_ADD(sub.end_date, 10)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_window s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_type NOT IN ('search', 'click')
        )
)
SELECT
    id,
    customer_id,
    started_at,
    started_at_label,
    first_known_country_2,
    subscription_id,
    plan,
    monthly_fee
FROM not_flagged
