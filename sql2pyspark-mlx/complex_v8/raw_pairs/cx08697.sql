WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_type <> 'reopened'
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_type,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug
    FROM base
    WHERE actor = 'customer'
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IS NOT NULL
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        SUBSTRING(TRIM(actor_slug), 1, 3) AS actor_slug_prefix,
        SUBSTRING(TRIM(actor_slug), 1, 4) AS actor_slug_prefix_2
    FROM without_matches
),
filtered AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor = 'agent'
              AND t.event_at < TIMESTAMP '2025-02-24 12:00:00'
        )
)
SELECT
    id,
    ticket_id,
    event_type,
    actor_slug,
    actor_slug_prefix_2,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    CONCAT_WS('|', event_type, actor_slug) AS event_type_actor_slug_label
FROM filtered
ORDER BY event_type_actor_slug_label DESC NULLS FIRST, event_type_or_unknown DESC
