WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id IS NOT NULL OR actor <> 'agent')
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count_2
    FROM prepared s
)
SELECT
    actor,
    event_type,
    ticket_id,
    GROUPING(actor) AS is_total_actor,
    SUM(CASE WHEN event_type <> 'reopened' THEN 1 ELSE 0 END) AS n_event_type_reopened,
    SUM(event_count) AS total_event_count,
    COUNT(*) FILTER (WHERE event_type = 'reopened') AS n_event_type_reopened_2
FROM with_stats
GROUP BY CUBE(actor, event_type, ticket_id)
ORDER BY actor NULLS LAST, event_type NULLS LAST, ticket_id NULLS LAST
