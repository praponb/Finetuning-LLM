WITH prepared_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (actor NOT IN ('agent', 'customer') OR event_at >= TIMESTAMP '2025-02-06 11:00:00')
),
filtered AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM prepared_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_at < TIMESTAMP '2025-02-12 08:00:00'
        )
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at_2
    FROM filtered s
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        event_at,
        last_event_at_2,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        CONCAT(event_type, '#', CAST(ticket_id AS STRING)) AS event_type_ref
    FROM profiled
)
-- Final projection
SELECT
    id,
    event_at,
    last_event_at_2,
    event_at_label,
    event_type_ref
FROM tagged
ORDER BY last_event_at_2
