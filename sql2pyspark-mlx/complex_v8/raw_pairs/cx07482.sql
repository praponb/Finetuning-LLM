WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IS NOT NULL
      AND HOUR(event_at) BETWEEN 6 AND 10
),
with_details AS (
    -- Join tickets
    SELECT s.id, s.ticket_id, t.agent_id AS ticket_agent_id
    FROM filtered s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT id, ticket_id, ticket_agent_id
FROM with_details
ORDER BY ticket_id NULLS LAST, id
LIMIT 20
