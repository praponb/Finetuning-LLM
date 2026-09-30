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
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        ticket_id,
        event_type,
        MIN(hours_since_open) AS min_hours_since_open,
        ROUND(SUM(hours_since_open), 2) AS total_hours_since_open,
        COUNT(*) FILTER (WHERE event_type = 'replied') AS n_event_type_replied,
        MAX(hours_since_open) AS max_hours_since_open,
        COUNT(*) AS cnt
    FROM joined_ranges
    GROUP BY ticket_id, event_type
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM per_group
    WHERE min_hours_since_open > (SELECT MIN(min_hours_since_open) FROM per_group)
)
-- Final projection
SELECT
    ticket_id,
    event_type,
    total_hours_since_open,
    n_event_type_replied,
    cnt,
    COALESCE(cnt, 0) AS cnt_filled,
    ROUND(total_hours_since_open * 1.07, 2) AS total_hours_since_open_adj
FROM with_activity
WHERE cnt <= 10
ORDER BY total_hours_since_open_adj DESC, n_event_type_replied DESC NULLS FIRST
