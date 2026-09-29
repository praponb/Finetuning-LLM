WITH scoped_events AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-19 08:00:00'
      AND actor IN ('system', 'customer')
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM scoped_events s
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.event_type_actor_label,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type
    FROM with_counts s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_at >= TIMESTAMP '2025-01-25 09:00:00'
)
SELECT
    id,
    event_at,
    event_type,
    event_id,
    event_event_at,
    event_event_type,
    CASE WHEN event_event_type IN ('created', 'resolved') THEN 'group_a' WHEN event_event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_event_type_group
FROM exploded_detail
