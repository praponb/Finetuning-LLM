WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        SUBSTRING(TRIM(actor), 1, 3) AS actor_prefix,
        DATEDIFF(DATE '2025-12-31', event_at) AS days_since_event_at
    FROM ticket_events
    WHERE actor IN ('agent', 'customer')
),
combined_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor_prefix,
        s.days_since_event_at,
        t.closed_at AS ticket_closed_at
    FROM recent s
    JOIN tickets t ON t.id = s.ticket_id
),
latest AS (
    -- Keep the latest row per ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        actor_prefix,
        days_since_event_at,
        ticket_closed_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY ticket_id ORDER BY event_at DESC, id DESC) AS rn
        FROM combined_details
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor_prefix,
    days_since_event_at,
    CONCAT(actor_prefix, '#', CAST(ticket_id AS STRING)) AS actor_prefix_ref
FROM latest
WHERE HOUR(ticket_closed_at) BETWEEN 9 AND 14
ORDER BY id NULLS LAST, ticket_id DESC
