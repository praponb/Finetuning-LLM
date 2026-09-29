WITH scoped_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        ticket_id,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        UPPER(TRIM(actor)) AS actor_clean,
        CONCAT_WS('-', event_type, actor) AS event_type_actor_label
    FROM scoped_events
),
enriched_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.csat AS ticket_csat,
        t.requester_id AS ticket_requester_id,
        t.opened_at AS ticket_opened_at
    FROM derived s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    actor_or_unknown,
    actor_clean,
    ticket_requester_id,
    ticket_opened_at
FROM enriched_ticket
WHERE actor NOT IN ('customer', 'agent')
ORDER BY actor_clean DESC, actor_or_unknown DESC NULLS FIRST, id
LIMIT 20
