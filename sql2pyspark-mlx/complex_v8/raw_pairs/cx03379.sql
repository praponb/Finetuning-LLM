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
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.actor = 'system' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM in_window s
)
-- Subtotals with ROLLUP
SELECT
    agent_id,
    COALESCE(priority, 'ALL') AS priority,
    COALESCE(event_type, 'ALL') AS event_type,
    MAX(event_at) AS last_event_at_2
FROM with_stats
GROUP BY ROLLUP(agent_id, priority, event_type)
ORDER BY agent_id NULLS LAST, priority NULLS LAST, event_type NULLS LAST
