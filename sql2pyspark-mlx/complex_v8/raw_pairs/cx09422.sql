WITH valid_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LENGTH(TRIM(event_type)) AS event_type_len,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE event_type IN ('created', 'resolved')
),
screened AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM valid_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type = 'replied'
              AND t.actor = 'agent'
        )
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM screened
    WHERE event_type_len < (SELECT MAX(event_type_len) FROM screened)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_type_len,
    actor_ref
FROM without_matches
WHERE TO_DATE(event_at) < DATE '2025-02-12'
ORDER BY event_at
