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
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        priority,
        agent_id,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open,
        MIN(event_at) AS first_event_at,
        SUM(hours_since_open) FILTER (WHERE event_type = 'resolved') AS hours_since_open_event_type_resolved,
        COUNT(*) FILTER (WHERE priority <> 'P3') AS n_priority_p3
    FROM in_window
    GROUP BY priority, agent_id
)
SELECT
    priority,
    agent_id,
    median_hours_since_open,
    first_event_at,
    hours_since_open_event_type_resolved,
    n_priority_p3,
    ABS(n_rows - 5) AS n_rows_distance
FROM summary
ORDER BY first_event_at DESC
