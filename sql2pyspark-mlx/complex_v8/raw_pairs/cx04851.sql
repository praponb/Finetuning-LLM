WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE agent_id IS NOT NULL
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at,
        (SELECT COUNT(*) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_count,
        (SELECT COUNT(CASE WHEN t.actor IN ('agent', 'customer') THEN 1 END) FROM ticket_events t WHERE t.ticket_id = s.id) AS event_actor_count
    FROM cleaned_tickets s
),
summarized_rows AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.id) AS last_event_at_2
    FROM with_counts s
)
SELECT id, measure, value
FROM (
    SELECT id, event_count, csat, event_actor_count
    FROM summarized_rows
) src
UNPIVOT (
    value FOR measure IN (event_count, csat, event_actor_count)
) unp
ORDER BY id, measure
