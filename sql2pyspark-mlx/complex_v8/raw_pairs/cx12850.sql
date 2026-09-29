WITH recent_events AS (
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
    SELECT *, SUM(starts_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        ticket_id,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, burst_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(session_end)) / 60, 1) AS minutes_session_end_to_session_start,
        DATEDIFF(DATE '2025-12-31', session_end) AS days_since_session_end
    FROM session_summary
)
-- Final projection
SELECT
    ticket_id,
    burst_id,
    session_start,
    session_end,
    events,
    duration_min,
    minutes_session_end_to_session_start,
    days_since_session_end,
    CASE WHEN days_since_session_end >= 560 THEN 'HIGH' WHEN days_since_session_end >= 315 THEN 'MEDIUM' ELSE 'LOW' END AS days_since_session_end_tier
FROM scored
ORDER BY days_since_session_end_tier DESC NULLS FIRST, session_start NULLS LAST, ticket_id, burst_id
LIMIT 20
