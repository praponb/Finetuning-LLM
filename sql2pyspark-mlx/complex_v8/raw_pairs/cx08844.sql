WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
qualified AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type IN ('replied', 'assigned')
              AND t.actor IN ('customer', 'agent')
        )
),
event_stats AS (
    -- Per-ticket ticket_events metrics
    SELECT
        ticket_id,
        MAX(event_at) AS last_event_at,
        MAX(event_at) AS last_event_at_2
    FROM ticket_events
    GROUP BY ticket_id
),
profiled AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        t.last_event_at,
        t.last_event_at_2
    FROM qualified s
    LEFT JOIN event_stats t ON t.ticket_id = s.ticket_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    actor,
    last_event_at,
    DATE_TRUNC('DAY', event_at) AS event_at_day,
    CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
FROM profiled
ORDER BY last_event_at DESC, actor_ref DESC
