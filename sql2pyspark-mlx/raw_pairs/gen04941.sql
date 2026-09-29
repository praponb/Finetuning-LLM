WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_FORMAT(started_at, 'yyyy-MM-dd') started_at_label
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-03-01 14:00:00')
),
latest_match AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.browser,
        s.started_at,
        latest.web_event_event_time,
        latest.web_event_event_type
    FROM base s
    JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.web_event_event_time
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.country,
        d.city
    FROM kept s
    INNER JOIN customers d
        ON s.customer_id = d.id
    WHERE d.city IN ('London', 'Singapore', 'Tokyo')
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id) > INTERVAL '60' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM enriched
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY web_event_event_type ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
latest_match_sessions AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.started_at,
        s.country,
        s.city,
        s.session_id,
        x.page
    FROM sessions_sessions s
    LEFT JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY started_at DESC, id) AS dedup_rank
    FROM latest_match_sessions
    QUALIFY dedup_rank = 1
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    country,
    page,
    COUNT(session_id) OVER (PARTITION BY DATE_TRUNC('MONTH', started_at), page) started_at_period_count_session_id,
    SUM(session_id) OVER (PARTITION BY page ORDER BY session_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_session_id,
    LEAD(session_id, 1) OVER (PARTITION BY country ORDER BY dedup_rank, id) next_session_id
FROM deduped
