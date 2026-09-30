WITH filtered_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', actor, event_type) AS actor_event_type_label,
        CASE WHEN event_type IN ('escalated', 'resolved') THEN 'group_a' WHEN event_type = 'assigned' THEN 'group_b' ELSE 'other' END AS event_type_group,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-25 10:00:00'
),
with_activity AS (
    -- Rows with no matching ticket_events
    SELECT s.*
    FROM filtered_events s
    WHERE NOT EXISTS (
            SELECT 1
            FROM ticket_events t
            WHERE t.ticket_id = s.ticket_id
              AND t.event_type <> 'assigned'
              AND HOUR(t.event_at) BETWEEN 8 AND 12
        )
      AND HOUR(s.event_at) BETWEEN 9 AND 12
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('DAY', event_at) AS event_at_day,
        CONCAT(actor, '#', CAST(id AS STRING)) AS actor_ref
    FROM with_activity
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_event_type_label,
    event_type_group,
    event_at_label,
    event_at_day,
    actor_ref,
    CASE WHEN event_type IN ('created', 'replied') THEN 'group_a' WHEN event_type = 'resolved' THEN 'group_b' ELSE 'other' END AS event_type_group_2,
    TO_DATE(event_at) AS event_at_date
FROM enriched
ORDER BY event_type_group_2 DESC
