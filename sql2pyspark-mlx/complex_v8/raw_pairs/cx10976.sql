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
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(hours_since_open - AVG(hours_since_open) OVER (PARTITION BY priority), 2) AS hours_since_open_vs_priority_avg,
        SUM(hours_since_open) OVER (PARTITION BY priority ORDER BY hours_since_open DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_hours_since_open,
        hours_since_open - LAG(hours_since_open) OVER (PARTITION BY priority ORDER BY hours_since_open DESC, id) AS hours_since_open_change
    FROM joined_ranges
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    agent_id,
    hours_since_open,
    hours_since_open_change,
    LEAST(hours_since_open, hours_since_open_change) AS least_hours_since_open_hours_since_open_change
FROM metrics
ORDER BY ticket_id, event_type, id
LIMIT 10
