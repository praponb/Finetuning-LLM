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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        ROUND(hours_since_open / NULLIF(SUM(hours_since_open) OVER (PARTITION BY priority, ticket_id), 0), 4) AS hours_since_open_share_of_priority_ticket_id,
        LAG(hours_since_open) OVER w_all_by_hours_since_open AS prev_hours_since_open
    FROM in_window
    WINDOW w_priority_ticket_id AS (PARTITION BY priority, ticket_id), w_all_by_hours_since_open AS (ORDER BY hours_since_open DESC NULLS FIRST, id)
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    hours_since_open_share_of_priority_ticket_id,
    prev_hours_since_open,
    prev_hours_since_open - hours_since_open AS prev_hours_since_open_minus_hours_since_open
FROM metrics
ORDER BY prev_hours_since_open_minus_hours_since_open DESC
