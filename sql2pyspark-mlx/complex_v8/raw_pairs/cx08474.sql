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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN priority IN ('P2', 'P3') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group,
        DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at
    FROM in_window
),
without_matches AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM tagged s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND MONTH(t.opened_at) IN (2, 3)
        )
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    priority_group,
    days_since_event_at,
    CONCAT(priority, '#', CAST(days_since_event_at AS STRING)) AS priority_ref
FROM without_matches
WHERE hours_since_open <= 57.6
ORDER BY agent_id DESC
