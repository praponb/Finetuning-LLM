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
scored AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        event_type,
        priority,
        agent_id,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h,
        CONCAT_WS('-', priority, event_type) AS priority_event_type_label
    FROM joined_ranges
)
SELECT
    id,
    event_at,
    event_type,
    priority,
    agent_id,
    priority_slug,
    event_at_plus_2h,
    priority_event_type_label
FROM scored
ORDER BY priority_slug DESC, event_type
