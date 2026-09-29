WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        DATEDIFF(CURRENT_DATE(), started_at) days_since_started_at,
        COALESCE(country, 'N/A') country_clean
    FROM sessions
    WHERE country IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.session_id IN (16, 31, 33, 37)
        )
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.days_since_started_at,
        s.country_clean,
        top1.web_event_page
    FROM eligible s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.device,
        src.days_since_started_at,
        d.name,
        d.age,
        d.signup_date
    FROM latest_match src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
)
-- Join a dimension and aggregate
SELECT
    dim.segment,
    a.device,
    COUNT(*) AS rows_total,
    ROUND(SUM(a.days_since_started_at), 2) AS total_days_since_started_at,
    GROUPING_ID() AS grouping_level
FROM with_dims a
LEFT JOIN customers dim ON a.customer_id = dim.id
GROUP BY CUBE (dim.segment, a.device);
