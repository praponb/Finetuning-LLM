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
tagged AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        priority,
        agent_id,
        hours_since_open,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix,
        SUBSTRING(TRIM(event_type), 1, 4) AS event_type_prefix_2
    FROM joined_ranges
),
stats AS (
    -- Group-level totals
    SELECT
        event_type,
        SUM(hours_since_open) AS total_hours_since_open,
        MIN(hours_since_open) AS min_hours_since_open
    FROM tagged
    GROUP BY event_type
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM stats
    WHERE total_hours_since_open > (SELECT AVG(total_hours_since_open) FROM stats)
)
-- Final projection
SELECT event_type, total_hours_since_open
FROM filtered
ORDER BY event_type
LIMIT 20
