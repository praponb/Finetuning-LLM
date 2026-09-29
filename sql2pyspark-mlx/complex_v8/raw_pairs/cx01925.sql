WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_TRUNC('QUARTER', event_at) AS event_at_quarter
    FROM ticket_events
    WHERE actor <> 'agent'
      AND event_type IN ('resolved', 'replied', 'created')
)
SELECT actor AS label, event_type AS name
FROM prepared
UNION
SELECT event_type AS label, actor AS name
FROM ticket_events
ORDER BY label DESC, name
