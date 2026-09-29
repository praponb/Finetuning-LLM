WITH latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.event_time
    FROM sessions s
    JOIN LATERAL (
            SELECT c.event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match_sessions AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        x.web_event_page
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.page IN ('/home', '/cart', '/checkout')
        )
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT_WS('/', browser, web_event_page) browser_web_event_page_key,
        COALESCE(browser, 'OTHER') browser_clean
    FROM not_flagged
    WHERE web_event_page IN ('/product', '/checkout')
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.browser,
        s.web_event_page,
        s.browser_web_event_page_key,
        top1.event_time,
        top1.event_type
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.event_time, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', event_time) AS year_start,
        event_type,
        MAX(event_time) AS last_seen,
        MIN(event_time) AS started_at,
        SIZE(COLLECT_SET(web_event_page)) AS num_web_event_pages,
        COUNT(DISTINCT web_event_page) AS distinct_web_event_pages
    FROM with_last
    GROUP BY DATE_TRUNC('YEAR', event_time), event_type
)
SELECT
    year_start,
    event_type,
    last_seen,
    started_at
FROM stats
ORDER BY event_type
