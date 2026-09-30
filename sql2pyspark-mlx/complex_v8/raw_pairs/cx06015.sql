WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id <= 31
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
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
joined_ticket AS (
    -- Join tickets
    SELECT
        s.ticket_id,
        s.burst_id,
        s.session_start,
        s.session_end,
        s.events,
        s.duration_min,
        t.priority AS ticket_priority,
        t.opened_at AS ticket_opened_at,
        t.tags AS ticket_tags
    FROM session_summary s
    JOIN tickets t ON t.id = s.ticket_id AND t.opened_at >= TIMESTAMP '2025-01-31 09:00:00'
)
SELECT
    ticket_id,
    session_start,
    session_end,
    duration_min,
    ticket_priority,
    ticket_tags,
    SUBSTRING(TRIM(ticket_priority), 1, 4) AS ticket_priority_prefix,
    ROUND(burst_id / NULLIF(events, 0), 3) AS burst_id_per_events
FROM joined_ticket
WHERE duration_min > 96.0
ORDER BY burst_id_per_events DESC, ticket_priority DESC
