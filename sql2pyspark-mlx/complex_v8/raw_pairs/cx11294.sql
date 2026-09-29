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
totals AS (
    -- Group-level totals
    SELECT
        event_type,
        agent_id,
        ticket_id,
        COUNT(*) AS n_rows,
        ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list
    FROM in_window
    GROUP BY event_type, agent_id, ticket_id
    HAVING MAX(hours_since_open) > 21.6
)
SELECT
    event_type,
    agent_id,
    ticket_id,
    n_rows,
    event_type_list,
    stddev_hours_since_open,
    priority_list,
    ABS(stddev_hours_since_open - 43.2) AS stddev_hours_since_open_distance
FROM totals
ORDER BY agent_id DESC, ticket_id DESC
