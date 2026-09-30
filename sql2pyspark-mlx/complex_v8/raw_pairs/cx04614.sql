WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-02-24'
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DENSE_RANK() OVER w_all_by_event_at_ties AS event_at_dense_rank,
        ROUND((UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(LAG(event_at) OVER (ORDER BY event_at DESC NULLS FIRST, id))) / 60, 1) AS minutes_since_prev_event_at
    FROM filtered_events
    WINDOW w_all_by_event_at_ties AS (ORDER BY event_at DESC NULLS FIRST)
),
detail_rows_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.minutes_since_prev_event_at,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM ranked s
    JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.event_type IN ('resolved', 'assigned', 'escalated')
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    event_event_type,
    event_actor
FROM detail_rows_events
WHERE event_at < TIMESTAMP '2025-03-08 08:00:00'
ORDER BY event_event_type, ticket_id DESC NULLS FIRST
