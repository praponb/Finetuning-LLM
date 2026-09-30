WITH recent_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type <> 'assigned'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
    FROM recent_events
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        event_at,
        actor,
        event_type_or_unknown,
        days_since_event_at,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        CASE WHEN days_since_event_at >= 595 THEN 'A' WHEN days_since_event_at >= 245 THEN 'B' ELSE 'C' END AS days_since_event_at_tier
    FROM labeled
    WHERE days_since_event_at > 140
)
-- Final projection
SELECT
    id,
    event_at,
    actor,
    days_since_event_at,
    event_at_label,
    days_since_event_at_tier
FROM scored
ORDER BY event_at_label
