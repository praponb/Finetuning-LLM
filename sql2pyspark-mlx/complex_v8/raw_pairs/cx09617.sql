WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type <> 'created'
      AND ticket_id >= 21
),
enriched_ticket AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.priority AS ticket_priority,
        t.closed_at AS ticket_closed_at,
        t.tags AS ticket_tags
    FROM base s
    JOIN tickets t ON t.id = s.ticket_id
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM enriched_ticket s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND HOUR(t.event_at) BETWEEN 10 AND 12
        )
)
SELECT
    actor,
    event_type,
    ticket_id,
    SUM(CASE WHEN actor IS NOT NULL THEN 1 ELSE 0 END) AS n_actor_is_not_null
FROM without_matches
GROUP BY GROUPING SETS ((actor, event_type, ticket_id), (actor), ())
ORDER BY actor NULLS LAST, event_type NULLS LAST, ticket_id NULLS LAST
