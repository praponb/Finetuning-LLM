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
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        priority,
        hours_since_open,
        ROUND(CUME_DIST() OVER w_all_by_event_at, 3) AS event_at_cume_dist,
        MIN(hours_since_open) OVER (ORDER BY hours_since_open, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_hours_since_open_so_far,
        SIZE(COLLECT_SET(priority) OVER (PARTITION BY event_type)) AS distinct_priority_in_event_type,
        LEAD(hours_since_open) OVER (ORDER BY event_at DESC, id) AS next_hours_since_open,
        LEAD(hours_since_open, 3, 0) OVER (ORDER BY hours_since_open, id) AS next3_hours_since_open,
        ROUND(AVG(hours_since_open) OVER (ORDER BY event_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS hours_since_open_avg_6_prior
    FROM joined_ranges
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
)
SELECT
    id,
    priority,
    hours_since_open,
    min_hours_since_open_so_far,
    distinct_priority_in_event_type,
    next_hours_since_open,
    next3_hours_since_open,
    hours_since_open_avg_6_prior,
    ROUND(next_hours_since_open / NULLIF(hours_since_open, 0), 3) AS next_hours_since_open_per_hours_since_open
FROM windowed
