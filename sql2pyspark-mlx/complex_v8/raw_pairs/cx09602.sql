WITH recent AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        SUBSTRING(TRIM(actor), 1, 4) AS actor_prefix
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor IS NOT NULL
),
without_matches AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM recent s
    WHERE EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor IN ('customer', 'system')
              AND t.event_type = 'escalated'
        )
),
with_details AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.actor_or_unknown,
        s.actor_prefix,
        t.tags AS ticket_tags,
        t.agent_id AS ticket_agent_id
    FROM without_matches s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Final projection
SELECT
    id,
    event_at,
    actor_prefix,
    ticket_tags,
    ticket_agent_id
FROM with_details
WHERE actor_or_unknown <> 'UNKNOWN'
ORDER BY event_at NULLS LAST, id
LIMIT 10
