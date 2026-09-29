WITH joined_ranges AS (
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
with_stats AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        (SELECT MIN(t.opened_at) FROM tickets t WHERE t.agent_id = s.agent_id) AS first_ticket_opened_at,
        (SELECT MAX(t.requester_id) FROM tickets t WHERE t.agent_id = s.agent_id) AS max_ticket_requester_id,
        (SELECT COUNT(*) FROM tickets t WHERE t.agent_id = s.agent_id) AS ticket_count
    FROM joined_ranges s
)
SELECT *
FROM (
    SELECT ticket_id, event_type, priority, ticket_count
    FROM with_stats
) src
PIVOT (
    MIN(ticket_count)
    FOR priority IN ('P2', 'P3', 'P1')
)
ORDER BY ticket_id, event_type
