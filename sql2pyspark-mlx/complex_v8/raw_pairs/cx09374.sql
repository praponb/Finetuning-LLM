WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        DAYOFWEEK(event_at) AS event_at_dayofweek,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_dayofweek,
        t.requester_id AS ticket_requester_id
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN t.actor = 'system' THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_actor_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM with_details s
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor NOT IN ('agent', 'customer')
        )
      AND s.event_actor_count < 5
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_actor_count,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM filtered s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type = 'escalated'
)
-- Final projection
SELECT
    id,
    event_actor_count,
    event_actor,
    CASE WHEN event_actor_count >= 7 THEN 'HIGH' WHEN event_actor_count >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS event_actor_count_tier
FROM line_level
WHERE event_actor NOT IN ('customer', 'agent')
ORDER BY event_actor_count, event_actor
