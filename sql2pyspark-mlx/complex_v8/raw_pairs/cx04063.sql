WITH filtered_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM') AS event_at_label,
        INITCAP(TRIM(event_type)) AS event_type_clean
    FROM ticket_events
    WHERE event_at >= TIMESTAMP '2025-01-19 11:00:00'
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        event_at_label,
        event_type_clean,
        FIRST_VALUE(event_at) IGNORE NULLS OVER (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_at,
        LAG(event_at, 3) OVER w_ticket_id_actor_by_event_at_2 AS prev3_event_at
    FROM filtered_events
    WINDOW w_ticket_id_actor_by_event_at_2 AS (PARTITION BY ticket_id, actor ORDER BY event_at DESC, id)
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type_clean,
    first_known_event_at,
    prev3_event_at,
    LENGTH(TRIM(event_type)) AS event_type_len
FROM ranked
WHERE event_type = 'assigned'
ORDER BY event_at DESC NULLS FIRST, event_type_len NULLS LAST
