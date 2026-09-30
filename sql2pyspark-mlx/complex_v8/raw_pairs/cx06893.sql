WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM recent_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        ticket_id,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_no
),
labeled AS (
    -- Label and score rows
    SELECT
        ticket_id,
        session_no,
        session_start,
        session_end,
        event_count,
        duration_min,
        ABS(duration_min - 72.0) AS duration_min_distance,
        COALESCE(duration_min, 0.0) AS duration_min_filled
    FROM sessions
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        COUNT(*) AS event_count_2,
        COUNT(*) AS event_count_3,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count_2, 0) AS event_count_2,
        COALESCE(t.event_count_3, 0) AS event_count_3,
        t.last_event_at
    FROM labeled s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Subtotals with ROLLUP
SELECT
    event_count_2,
    ticket_id,
    event_count_3,
    GROUPING(event_count_2) AS is_total_event_count_2,
    MIN(session_start) AS first_session_start
FROM with_stats
GROUP BY ROLLUP(event_count_2, ticket_id, event_count_3)
