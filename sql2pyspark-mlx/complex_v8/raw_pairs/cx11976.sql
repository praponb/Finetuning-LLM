WITH base AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-31 09:00:00'
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 90 THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        ticket_id,
        session_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, session_id
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.ticket_id,
        s.session_id,
        s.session_end,
        s.duration_min,
        t.priority AS ticket_priority
    FROM session_summary s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND TO_DATE(t.closed_at) < DATE '2025-02-22'
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.requester_id AS ticket_requester_id,
        t.closed_at AS ticket_closed_at,
        su.hired_on AS agent_hired_on
    FROM enriched_ticket s
    JOIN tickets t ON t.id = s.ticket_id
    JOIN support_agents su ON su.id = t.agent_id
    WHERE TO_DATE(t.closed_at) < DATE '2025-02-22'
)
-- Final projection
SELECT
    ticket_id,
    session_id,
    session_end,
    duration_min,
    ticket_closed_at,
    HOUR(ticket_closed_at) AS ticket_closed_at_hour,
    FLOOR(duration_min / 50) * 50 AS duration_min_bucket
FROM enriched
WHERE session_end >= TIMESTAMP '2025-01-13 09:00:00'
