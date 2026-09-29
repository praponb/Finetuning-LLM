WITH covered AS (
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
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN priority IN ('P2', 'P1') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group,
        COALESCE(hours_since_open, 0.0) AS hours_since_open_filled
    FROM covered
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    agent_id,
    hours_since_open,
    hours_since_open_filled
FROM tagged
ORDER BY id DESC NULLS FIRST
LIMIT 5
