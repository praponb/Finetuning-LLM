WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug
    FROM ticket_events
    WHERE HOUR(event_at) BETWEEN 8 AND 11
      AND event_type = 'resolved'
)
SELECT event_type_slug, event_type
FROM valid
UNION
SELECT channel AS event_type_slug, priority AS event_type
FROM tickets
ORDER BY event_type
