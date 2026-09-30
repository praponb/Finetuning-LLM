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
ticket_id_stats AS (
    -- Avg hours_since_open per ticket_id
    SELECT ticket_id, AVG(hours_since_open) AS avg_hours_since_open
    FROM matched
    GROUP BY ticket_id
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_since_open,
        ROUND(s.hours_since_open - g.avg_hours_since_open, 2) AS hours_since_open_minus_avg
    FROM matched s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.hours_since_open > g.avg_hours_since_open
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    priority,
    agent_id,
    hours_since_open,
    avg_hours_since_open,
    hours_since_open_minus_avg,
    ROUND(avg_hours_since_open * 1.07, 2) AS avg_hours_since_open_adj
FROM outliers
WHERE avg_hours_since_open <= 50.4
