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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        event_at,
        priority,
        agent_id,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        CASE WHEN priority IN ('P4', 'P2') THEN 'group_a' WHEN priority = 'P1' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM joined_ranges
    WHERE agent_id <= 6
),
with_activity AS (
    -- Rows with no matching tickets
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel = 'phone'
              AND t.csat <= 4
        )
      AND s.priority_group <> 'group_a'
)
-- Final projection
SELECT
    id,
    event_at,
    priority,
    agent_id,
    event_at_day,
    event_at_label,
    priority_group,
    DATEDIFF(DATE '2026-01-01', event_at_day) AS days_since_event_at_day
FROM with_activity
ORDER BY id DESC
