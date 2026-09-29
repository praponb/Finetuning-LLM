WITH prepared AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE (ticket_id >= 33 OR event_type = 'replied')
),
with_counts AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM prepared s
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, t.csat AS ticket_csat
    FROM with_counts s
    LEFT JOIN tickets t ON t.id = s.ticket_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT event_type, actor, ticket_csat
    FROM combined_details
) src
PIVOT (
    AVG(ticket_csat)
    FOR actor IN ('customer', 'system')
)
