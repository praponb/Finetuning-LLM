WITH valid AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'replied')
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM valid s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.event_at >= TIMESTAMP '2025-01-31 10:00:00'
              AND t.event_type IN ('created', 'replied', 'assigned')
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        INITCAP(TRIM(event_type)) AS event_type_clean,
        CONCAT(actor_clean, '#', CAST(ticket_id AS STRING)) AS actor_clean_ref
    FROM with_activity
)
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    actor_clean,
    actor_ref,
    event_type_clean,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
    COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown_2
FROM annotated
ORDER BY id
