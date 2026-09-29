WITH matched AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
qualified AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.opened_at < TIMESTAMP '2025-02-06 15:00:00'
              AND t.requester_id >= 7
        )
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count,
        (SELECT MAX(t.closed_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS last_ticket_closed_at
    FROM qualified s
),
with_activity AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM profiled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.opened_at >= TIMESTAMP '2025-01-13 07:00:00'
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    agent_id,
    hours_since_open,
    ticket_count,
    last_ticket_closed_at,
    DATE_TRUNC('HOUR', last_ticket_closed_at) AS last_ticket_closed_at_hour
FROM with_activity
