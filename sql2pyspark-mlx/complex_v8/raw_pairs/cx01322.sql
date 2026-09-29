WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 6 AND 10
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        INITCAP(TRIM(event_type)) AS event_type_clean,
        LOWER(TRIM(actor)) AS actor_clean
    FROM prepared
)
-- Subtotals with GROUPING SETS
SELECT
    actor,
    event_type,
    ticket_id,
    MAX(event_at) AS last_event_at
FROM scored
GROUP BY GROUPING SETS ((actor, event_type, ticket_id), (actor), (event_type))
