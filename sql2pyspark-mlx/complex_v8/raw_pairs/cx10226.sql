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
joined_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.channel AS ticket_channel
    FROM matched s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
ticket_channel_stats AS (
    -- Median hours_since_open per ticket_channel
    SELECT ticket_channel, PERCENTILE_APPROX(hours_since_open, 0.5) AS median_hours_since_open
    FROM joined_ticket
    GROUP BY ticket_channel
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_hours_since_open,
        ROUND(s.hours_since_open - g.median_hours_since_open, 2) AS hours_since_open_minus_median
    FROM joined_ticket s
    JOIN ticket_channel_stats g ON g.ticket_channel = s.ticket_channel
    WHERE s.hours_since_open >= g.median_hours_since_open
),
summary AS (
    -- Group-level totals
    SELECT
        priority,
        ROUND(SUM(hours_since_open_minus_median), 2) AS total_hours_since_open_minus_median,
        SUM(median_hours_since_open) FILTER (WHERE event_type <> 'created') AS median_hours_since_open_event_type_created,
        SUM(hours_since_open_minus_median) AS total_hours_since_open_minus_median_2,
        SUM(CASE WHEN event_type = 'resolved' THEN 1 ELSE 0 END) AS n_event_type_resolved,
        ROUND(SUM(hours_since_open), 2) AS total_hours_since_open
    FROM above_average
    GROUP BY 1
    HAVING SUM(median_hours_since_open) > 28.8
)
-- Final projection
SELECT
    priority,
    total_hours_since_open_minus_median,
    median_hours_since_open_event_type_created,
    n_event_type_resolved,
    ROUND(n_event_type_resolved / NULLIF(total_hours_since_open, 0), 3) AS n_event_type_resolved_per_total_hours_since_open
FROM summary
WHERE total_hours_since_open_minus_median_2 <= 306.0
ORDER BY median_hours_since_open_event_type_created, n_event_type_resolved_per_total_hours_since_open
