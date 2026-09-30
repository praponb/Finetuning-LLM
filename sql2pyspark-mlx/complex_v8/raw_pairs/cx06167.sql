WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type = 'assigned'
      AND actor IS NOT NULL
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.actor,
        t.agent_id AS ticket_agent_id,
        t.priority AS ticket_priority
    FROM scoped s
    LEFT JOIN tickets t ON t.id = s.ticket_id AND t.priority NOT IN ('P4', 'P2')
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        LENGTH(TRIM(actor)) AS actor_len
    FROM combined_details_ticket
    WHERE ticket_priority = 'P2'
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    event_type_or_unknown,
    actor_len
FROM labeled
ORDER BY actor_len DESC NULLS FIRST, id
LIMIT 10
