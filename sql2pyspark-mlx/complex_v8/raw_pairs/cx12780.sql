WITH prepared_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('-', actor, event_type) AS actor_event_type_label,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label_2
    FROM ticket_events
    WHERE event_type = 'reopened'
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor_event_type_label_2,
        QUARTER(event_at) AS event_at_quarter
    FROM prepared_events
    WHERE actor_event_type_label IS NOT NULL
)
SELECT
    event_type,
    event_at_quarter,
    GROUPING(event_type) AS is_total_event_type,
    COUNT(*) AS n_rows,
    ROUND(AVG(event_at_quarter), 2) AS avg_event_at_quarter,
    ROUND(100 * COUNT(CASE WHEN event_type = 'resolved' THEN 1 END) / COUNT(*), 1) AS pct_event_type_resolved
FROM scored
GROUP BY GROUPING SETS ((event_type, event_at_quarter), (event_type), (event_at_quarter), ())
ORDER BY event_type NULLS LAST, event_at_quarter NULLS LAST
