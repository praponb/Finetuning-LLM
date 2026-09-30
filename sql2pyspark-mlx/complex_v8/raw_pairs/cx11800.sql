WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM ticket_events
    WHERE actor IN ('customer', 'agent')
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 10 AND 15
        )
      AND s.event_type NOT IN ('escalated', 'created')
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MIN(event_at) AS first_event_at,
        MAX(event_at) AS last_event_at
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.first_event_at,
        t.last_event_at
    FROM without_matches s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_slug,
    first_event_at,
    last_event_at,
    SUBSTRING(TRIM(actor_slug), 1, 3) AS actor_slug_prefix
FROM profiled
ORDER BY ticket_id DESC, event_at, id
LIMIT 10
