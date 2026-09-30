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
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        ticket_id,
        event_at,
        priority,
        hours_since_open,
        MAX(hours_since_open) OVER (PARTITION BY event_type, priority) AS event_type_priority_max_hours_since_open,
        ROUND(AVG(hours_since_open) OVER w_event_type_priority, 2) AS event_type_priority_avg_hours_since_open,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id ORDER BY event_at DESC, id), 3) AS event_at_cume_dist,
        NTH_VALUE(event_at, 2) OVER (PARTITION BY priority, event_type ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_event_at
    FROM joined_ranges
    WINDOW w_event_type_priority AS (PARTITION BY event_type, priority)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    hours_since_open,
    event_type_priority_max_hours_since_open,
    event_type_priority_avg_hours_since_open,
    event_at_cume_dist,
    second_event_at,
    ROUND(hours_since_open * 0.9, 2) AS hours_since_open_adj
FROM sequenced
ORDER BY priority DESC, event_at_cume_dist NULLS LAST, id
LIMIT 10
