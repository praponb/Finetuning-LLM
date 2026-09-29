WITH in_window AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MIN(opened_at) AS first_ticket_opened_at
    FROM tickets
    GROUP BY agent_id
),
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.first_ticket_opened_at
    FROM in_window s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM with_stats s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    priority,
    agent_id,
    hours_since_open,
    event_event_at
FROM detail_rows_events
WHERE event_event_at < TIMESTAMP '2025-03-02 15:00:00'
ORDER BY priority DESC, id
