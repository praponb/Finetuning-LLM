WITH filtered AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE actor IS NOT NULL
)
SELECT ticket_id AS entity_id, id AS qty, event_at
FROM filtered
UNION ALL
SELECT id AS entity_id, ticket_id AS qty, event_at
FROM ticket_events
WHERE HOUR(event_at) BETWEEN 8 AND 14
