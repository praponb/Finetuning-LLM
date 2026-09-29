WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM scoped_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.prev_event_at,
        s.is_new_session,
        s.session_id,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM sessionized s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IN ('created', 'resolved')
),
stats AS (
    -- Group-level totals
    SELECT
        TO_DATE(event_event_at) AS event_event_at_day,
        DATE_FORMAT(prev_event_at, 'yyyy-MM') AS prev_event_at_ym,
        COUNT(*) AS n_rows,
        COUNT(*) FILTER (WHERE event_event_type IN ('reopened', 'replied')) AS n_event_event_type_in_reopened,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(event_event_at) AS first_event_event_at
    FROM line_level
    WHERE event_event_type <> 'replied'
    GROUP BY TO_DATE(event_event_at), DATE_FORMAT(prev_event_at, 'yyyy-MM')
    HAVING AVG(is_new_session) > 0
)
-- Final projection
SELECT
    event_event_at_day,
    n_rows,
    n_event_event_type_in_reopened,
    first_event_event_at,
    LOWER(REGEXP_REPLACE(TRIM(prev_event_at_ym), ' ', '_')) AS prev_event_at_ym_slug
FROM stats
WHERE TO_DATE(first_event_event_at) < DATE '2025-01-31'
ORDER BY event_event_at_day, prev_event_at_ym_slug DESC
