WITH filtered AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND event_type IN ('replied', 'assigned', 'resolved')
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN event_type IN ('assigned', 'escalated') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM filtered
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    actor_or_unknown,
    event_type_group
FROM scored
ORDER BY actor_or_unknown DESC NULLS FIRST
