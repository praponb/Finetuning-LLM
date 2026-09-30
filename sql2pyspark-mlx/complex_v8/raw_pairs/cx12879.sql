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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY priority, ticket_id) AS rows_in_priority_ticket_id,
        ROUND(CUME_DIST() OVER w_event_type_ticket_id_by_hour, 3) AS hours_since_open_cume_dist,
        hours_since_open - LAG(hours_since_open) OVER (PARTITION BY event_type, ticket_id ORDER BY hours_since_open, id) AS hours_since_open_change
    FROM in_window
    WINDOW w_event_type_ticket_id_by_hour AS (PARTITION BY event_type, ticket_id ORDER BY hours_since_open, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    hours_since_open,
    hours_since_open_change,
    CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
    DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
FROM windowed
WHERE priority NOT IN ('P2', 'P4')
ORDER BY priority_ref, ticket_id, id
LIMIT 5
