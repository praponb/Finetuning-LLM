WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE actor NOT IN ('system', 'agent')
      AND event_type NOT IN ('reopened', 'escalated')
),
qualified AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM valid s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_type <> 'assigned'
              AND t.actor IN ('system', 'agent')
        )
),
filtered AS (
    -- Keep rows whose ticket matches
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.id = s.ticket_id
              AND t.priority IS NOT NULL
        )
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM filtered
ORDER BY event_type_len, actor DESC NULLS FIRST
