WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        SUM(is_new_session) OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        browser,
        session_id,
        MAX(session_id) OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_session_id,
        COUNT(session_id) OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_session_id,
        SUM(session_id) OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_session_id,
        MIN(session_id) OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_session_id,
        NTILE(3) OVER (ORDER BY session_id, id) AS session_id_bucket
    FROM sessions_sessions
    WHERE HOUR(started_at) BETWEEN 10 AND 14
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.browser,
        s.session_id_bucket,
        x.web_event_event_type
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_type IN ('search', 'add_to_cart', 'click')
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        browser,
        web_event_event_type,
        AVG(session_id_bucket) OVER (PARTITION BY web_event_event_type ORDER BY session_id_bucket, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS session_id_bucket_avg_6_prior,
        SUM(session_id_bucket) OVER (PARTITION BY web_event_event_type ORDER BY session_id_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_id_bucket
    FROM eligible
)
SELECT
    id,
    browser,
    web_event_event_type,
    session_id_bucket_avg_6_prior,
    running_session_id_bucket,
    CASE WHEN browser RLIKE '^(Safari|Edge)$' THEN 'EXTENDED' WHEN browser RLIKE '^(Chrome|Firefox)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS browser_zone
FROM scored;
