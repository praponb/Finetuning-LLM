WITH base_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        event_at + INTERVAL 7 DAYS AS event_at_plus_7d
    FROM ticket_events
    WHERE event_at < TIMESTAMP '2025-02-24 12:00:00'
      AND ticket_id IS NOT NULL
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown
    FROM base_events
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type_or_unknown,
    YEAR(event_at) AS event_at_year,
    CONCAT(event_type_or_unknown, '#', CAST(id AS STRING)) AS event_type_or_unknown_ref
FROM derived
ORDER BY id DESC, event_type_or_unknown
