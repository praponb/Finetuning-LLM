WITH base_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE event_type IN ('created', 'assigned', 'resolved')
),
derived AS (
    -- Add calculated columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at,
        CASE WHEN event_type IN ('resolved', 'replied') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group,
        SUBSTRING(TRIM(event_type), 1, 3) AS event_type_prefix
    FROM base_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT MIN(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS first_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM derived s
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    days_since_event_at,
    event_type_group,
    event_type_prefix,
    last_event_at,
    event_count
FROM with_counts
WHERE event_type_prefix IS NOT NULL
ORDER BY actor NULLS LAST, last_event_at DESC, id
LIMIT 20
