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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_id,
        priority,
        SUM(CASE WHEN priority = 'P2' THEN hours_since_open ELSE 0 END) AS hours_since_open_priority_p2,
        MIN(hours_since_open) AS min_hours_since_open,
        COUNT(*) AS row_count
    FROM joined_ranges
    WHERE ticket_id >= 43
    GROUP BY 1, 2
)
-- Native SQL UNPIVOT (wide to long)
SELECT agent_id, priority, field, amount
FROM (
    SELECT agent_id, priority, min_hours_since_open, hours_since_open_priority_p2
    FROM summary
) src
UNPIVOT (
    amount FOR field IN (min_hours_since_open, hours_since_open_priority_p2)
) unp
