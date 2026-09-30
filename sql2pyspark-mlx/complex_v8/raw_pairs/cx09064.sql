WITH cleaned_events AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(event_type, 'UNKNOWN') AS event_type_or_unknown,
        CASE WHEN event_type IN ('created', 'assigned') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS event_count
    FROM cleaned_events s
)
-- Subtotals with GROUPING SETS
SELECT
    ticket_id,
    event_count,
    COUNT(*) AS n_rows,
    SUM(event_count) AS total_event_count
FROM summarized_rows
GROUP BY GROUPING SETS ((ticket_id, event_count), (ticket_id), ())
ORDER BY ticket_id NULLS LAST, event_count NULLS LAST
