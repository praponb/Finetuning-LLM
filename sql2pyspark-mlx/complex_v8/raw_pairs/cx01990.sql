WITH covered AS (
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
priority_stats AS (
    -- Avg hours_since_open per priority
    SELECT priority, AVG(hours_since_open) AS avg_hours_since_open
    FROM covered
    GROUP BY priority
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_since_open,
        ROUND(s.hours_since_open - g.avg_hours_since_open, 2) AS hours_since_open_minus_avg
    FROM covered s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.hours_since_open > g.avg_hours_since_open
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    avg_hours_since_open,
    hours_since_open_minus_avg,
    LOWER(TRIM(event_type)) AS event_type_clean,
    CONCAT_WS('|', priority, event_type) AS priority_event_type_label
FROM vs_group
WHERE avg_hours_since_open BETWEEN 3.6 AND 50.4
ORDER BY event_type_clean NULLS LAST, ticket_id DESC NULLS FIRST
