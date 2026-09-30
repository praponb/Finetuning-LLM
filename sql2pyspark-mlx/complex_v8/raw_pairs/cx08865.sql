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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
agent_id_stats AS (
    -- Median hours_since_open per agent_id
    SELECT agent_id, PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open
    FROM covered
    GROUP BY agent_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_hours_since_open,
        ROUND(s.hours_since_open - g.median_hours_since_open, 2) AS hours_since_open_minus_median
    FROM covered s
    JOIN agent_id_stats g ON g.agent_id = s.agent_id
    WHERE s.hours_since_open >= g.median_hours_since_open
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT ticket_id, COUNT(*) AS event_count
    FROM ticket_events
    GROUP BY ticket_id
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        COALESCE(t.event_count, 0) AS event_count
    FROM vs_group s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    priority,
    agent_id,
    hours_since_open,
    hours_since_open_minus_median
FROM summarized_rows
ORDER BY hours_since_open DESC
