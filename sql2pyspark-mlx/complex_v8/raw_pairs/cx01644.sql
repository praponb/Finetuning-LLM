WITH prepared_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
        LOWER(REGEXP_REPLACE(TRIM(actor), ' ', '_')) AS actor_slug,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref_2
    FROM ticket_events
    WHERE actor IN ('agent', 'system')
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h
    FROM prepared_events
    WHERE event_type = 'escalated'
),
deduped AS (
    -- Keep the latest row per actor, ticket_id
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_type_ref,
        actor_slug,
        event_type_ref_2,
        event_at_quarter,
        event_at_plus_2h
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY actor, ticket_id ORDER BY event_at DESC, id DESC) AS row_num
        FROM scored
    ) ranked
    WHERE row_num = 1
)
SELECT
    id,
    event_at,
    actor,
    actor_slug,
    event_type_ref_2,
    event_at_quarter,
    event_at_plus_2h,
    DATE_TRUNC('WEEK', event_at_plus_2h) AS event_at_plus_2h_week
FROM deduped
ORDER BY event_type_ref_2, actor_slug
