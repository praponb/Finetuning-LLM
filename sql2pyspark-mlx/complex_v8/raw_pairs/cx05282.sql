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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
event_type_stats AS (
    -- Avg hours_since_open per event_type
    SELECT event_type, AVG(hours_since_open) AS avg_hours_since_open
    FROM covered
    GROUP BY event_type
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_since_open,
        ROUND(s.hours_since_open - g.avg_hours_since_open, 2) AS hours_since_open_minus_avg
    FROM covered s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.hours_since_open > g.avg_hours_since_open
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        s.avg_hours_since_open,
        s.hours_since_open_minus_avg,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM outliers s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    agent_id,
    hours_since_open,
    avg_hours_since_open,
    hours_since_open_minus_avg,
    event_id,
    event_actor,
    event_event_at,
    event_event_type,
    QUARTER(event_at) AS event_at_quarter
FROM exploded_detail
ORDER BY event_at_quarter DESC
