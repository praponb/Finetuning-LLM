WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE actor <> 'system'
)
SELECT event_type, actor AS name, ticket_id
FROM scoped_events
UNION ALL
SELECT priority, channel, agent_id
FROM tickets
WHERE priority IN ('P1', 'P4')
  AND opened_at >= TIMESTAMP '2025-01-13 13:00:00'
ORDER BY name DESC, ticket_id
