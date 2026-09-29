WITH valid AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND actor = 'system'
),
with_children AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at
    FROM valid s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IS NOT NULL
),
without_matches AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.actor NOT IN ('agent', 'system')
        )
),
enriched_event AS (
    -- Attach event attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_id,
        s.event_event_type,
        s.event_event_at,
        t.event_at AS event_event_at_2,
        ti.priority AS ticket_priority,
        ti.closed_at AS ticket_closed_at
    FROM without_matches s
    JOIN ticket_events t ON t.id = s.event_id AND t.event_type IS NOT NULL
    JOIN tickets ti ON ti.id = t.ticket_id
    WHERE t.event_at < TIMESTAMP '2025-02-18 09:00:00'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', event_event_at_2) ORDER BY ticket_closed_at DESC) AS ticket_closed_at_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY event_type, ticket_id ORDER BY ticket_closed_at DESC), 3) AS ticket_closed_at_percent_rank
    FROM enriched_event
)
SELECT
    id,
    ticket_id,
    event_at,
    event_id,
    event_event_type,
    event_event_at,
    event_event_at_2,
    ticket_priority,
    ticket_closed_at,
    ticket_closed_at_dense_rank,
    ROUND((UNIX_TIMESTAMP(event_event_at) - UNIX_TIMESTAMP(event_at)) / 60, 1) AS minutes_event_at_to_event_event_at
FROM sequenced
ORDER BY ticket_closed_at NULLS LAST, ticket_priority
