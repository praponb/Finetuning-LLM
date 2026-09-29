WITH scoped_events AS (
    -- Scope the ticket_events rows
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
    FROM scoped_events
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
sessions AS (
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
with_parent_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.ticket_id,
        s.burst_id,
        s.events,
        s.duration_min,
        t.csat AS ticket_csat,
        t.tags AS ticket_tags
    FROM sessions s
    JOIN tickets t ON t.id = s.ticket_id
    WHERE t.csat < 4
)
-- Final projection
SELECT
    ticket_id,
    burst_id,
    events,
    ticket_tags,
    ABS(burst_id - 4) AS burst_id_distance
FROM with_parent_ticket
ORDER BY ticket_id DESC, events DESC
