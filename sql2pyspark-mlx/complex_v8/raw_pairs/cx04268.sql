WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE actor = 'customer'
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
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
scored AS (
    -- Add calculated columns
    SELECT
        ticket_id,
        session_no,
        session_end,
        event_count,
        duration_min,
        LEAST(session_no, event_count) AS least_session_no_event_count,
        DATE_TRUNC('DAY', session_end) AS session_end_day,
        ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 60, 1) AS minutes_session_end_to_session_start
    FROM sessions
)
SELECT
    ticket_id,
    session_no,
    event_count,
    duration_min,
    least_session_no_event_count,
    session_end_day,
    minutes_session_end_to_session_start,
    COALESCE(event_count, 0) AS event_count_filled,
    ROUND(duration_min * 0.25, 2) AS duration_min_adj
FROM scored
ORDER BY event_count, ticket_id, session_no
LIMIT 10
