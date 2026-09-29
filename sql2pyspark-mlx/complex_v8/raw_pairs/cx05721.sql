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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', event_at) AS event_at_month,
        ROUND(STDDEV(hours_since_open), 2) AS stddev_hours_since_open
    FROM matched
    GROUP BY DATE_TRUNC('MONTH', event_at)
)
SELECT
    event_at_month,
    stddev_hours_since_open,
    DATE_FORMAT(event_at_month, 'yyyy-MM') AS event_at_month_label
FROM stats
ORDER BY stddev_hours_since_open
