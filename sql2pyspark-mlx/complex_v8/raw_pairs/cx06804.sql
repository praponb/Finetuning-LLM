WITH matched AS (
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
per_group AS (
    -- Group-level totals
    SELECT
        priority,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at,
        PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open,
        COUNT(*) AS cnt,
        ROUND(AVG(hours_since_open), 2) AS avg_hours_since_open
    FROM matched
    GROUP BY priority
)
-- Native SQL UNPIVOT (wide to long)
SELECT priority, field, value
FROM (
    SELECT priority, avg_hours_since_open, median_hours_since_open
    FROM per_group
) src
UNPIVOT (
    value FOR field IN (avg_hours_since_open, median_hours_since_open)
) unp
