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
priority_stats AS (
    -- Avg hours_since_open per priority
    SELECT priority, AVG(hours_since_open) AS avg_hours_since_open
    FROM joined_ranges
    GROUP BY priority
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_hours_since_open,
        ROUND(s.hours_since_open - g.avg_hours_since_open, 2) AS hours_since_open_minus_avg
    FROM joined_ranges s
    JOIN priority_stats g ON g.priority = s.priority
    WHERE s.hours_since_open > g.avg_hours_since_open
)
SELECT
    id,
    event_at,
    agent_id,
    hours_since_open,
    DATE_TRUNC('WEEK', event_at) AS event_at_week,
    GREATEST(avg_hours_since_open, hours_since_open) AS greatest_avg_hours_since_open_hours_since_open
FROM compared
WHERE avg_hours_since_open > 25.2
