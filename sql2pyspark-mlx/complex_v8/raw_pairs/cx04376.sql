WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor <> 'customer'
      AND ticket_id IS NOT NULL
)
SELECT
    id AS ref_id,
    event_at,
    actor,
    'event' AS record_type
FROM cleaned
WHERE event_type IN ('created', 'assigned', 'reopened')
  AND ticket_id IS NOT NULL
UNION ALL
SELECT
    id,
    event_at,
    event_type,
    'event_2' AS record_type
FROM ticket_events
ORDER BY event_at
