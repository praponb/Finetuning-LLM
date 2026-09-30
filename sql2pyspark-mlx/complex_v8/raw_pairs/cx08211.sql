WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE event_type = 'resolved'
      AND ticket_id >= 30
),
ticket_id_stats AS (
    -- Max event_at_hour per ticket_id
    SELECT ticket_id, MAX(event_at_hour) AS max_event_at_hour
    FROM scoped_events
    GROUP BY ticket_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_event_at_hour,
        ROUND(s.event_at_hour - g.max_event_at_hour, 2) AS event_at_hour_minus_max
    FROM scoped_events s
    JOIN ticket_id_stats g ON g.ticket_id = s.ticket_id
    WHERE s.event_at_hour = g.max_event_at_hour
),
stats AS (
    -- Summarise per group
    SELECT
        event_type,
        ROUND(AVG(event_at_hour), 2) AS avg_event_at_hour,
        SUM(event_at_hour_minus_max) FILTER (WHERE actor <> 'agent') AS event_at_hour_minus_max_actor_agent,
        ROUND(SUM(max_event_at_hour) / NULLIF(SUM(event_at_hour), 0), 3) AS max_event_at_hour_to_event_at_hour_ratio,
        SUM(event_at_hour) AS total_event_at_hour
    FROM vs_group
    WHERE event_type <> 'resolved'
    GROUP BY event_type
    HAVING SUM(event_at_hour_minus_max) > -9.6
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM stats
    WHERE avg_event_at_hour > (SELECT MIN(avg_event_at_hour) FROM stats)
)
-- Final projection
SELECT
    event_type,
    avg_event_at_hour,
    event_at_hour_minus_max_actor_agent,
    max_event_at_hour_to_event_at_hour_ratio,
    total_event_at_hour
FROM screened
ORDER BY event_type DESC
