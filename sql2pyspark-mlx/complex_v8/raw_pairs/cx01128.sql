WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        HOUR(event_at) AS event_at_hour,
        TO_DATE(event_at) AS event_at_date,
        DATE_TRUNC('DAY', event_at) AS event_at_day
    FROM ticket_events
),
event_type_stats AS (
    -- Avg event_at_hour per event_type
    SELECT event_type, AVG(event_at_hour) AS avg_event_at_hour
    FROM prepared_events
    GROUP BY event_type
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_event_at_hour,
        ROUND(s.event_at_hour - g.avg_event_at_hour, 2) AS event_at_hour_minus_avg
    FROM prepared_events s
    JOIN event_type_stats g ON g.event_type = s.event_type
    WHERE s.event_at_hour > g.avg_event_at_hour
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM compared
    WHERE avg_event_at_hour > (SELECT AVG(avg_event_at_hour) FROM compared)
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_at_hour,
        s.event_at_date,
        s.event_at_day,
        s.avg_event_at_hour,
        s.event_at_hour_minus_avg,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_at AS event_event_at
    FROM with_activity s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor IS NOT NULL
)
-- Final projection
SELECT
    id,
    ticket_id,
    actor,
    event_at_hour,
    event_at_date,
    event_at_day,
    avg_event_at_hour,
    event_at_hour_minus_avg,
    event_id,
    event_actor,
    event_event_at
FROM exploded_detail
WHERE event_at_date >= DATE '2024-06-01'
ORDER BY avg_event_at_hour DESC, id, event_id
LIMIT 5
