WITH recent AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-03-08 09:00:00'
      AND actor = 'agent'
)
SELECT event_type AS name
FROM recent
INTERSECT
SELECT tier AS name
FROM support_agents
ORDER BY name
