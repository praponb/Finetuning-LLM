WITH scoped AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd') AS event_at_label,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref,
        LENGTH(TRIM(actor)) AS actor_len
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
      AND TO_DATE(event_at) < DATE '2025-02-12'
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.actor,
        s.event_at_label,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type AS event_event_type
    FROM scoped s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2
    FROM line_level s
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    actor,
    event_at_label,
    event_id,
    event_event_type,
    event_count,
    event_count_2,
    MONTH(event_at) AS event_at_month,
    CASE WHEN event_count_2 >= 6 THEN 'large' WHEN event_count_2 >= 3 THEN 'medium' ELSE 'small' END AS event_count_2_tier
FROM with_counts
ORDER BY event_count DESC
