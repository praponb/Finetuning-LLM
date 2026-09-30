WITH cleaned AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN event_type IN ('reopened', 'assigned') THEN 'group_a' WHEN event_type = 'escalated' THEN 'group_b' ELSE 'other' END AS event_type_group
    FROM ticket_events
    WHERE TO_DATE(event_at) < DATE '2025-03-14'
),
with_stats AS (
    -- Attach ticket_events metrics
    SELECT
        s.*,
        (SELECT MAX(t.event_at) FROM ticket_events t WHERE t.ticket_id = s.ticket_id) AS last_event_at
    FROM cleaned s
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT s.*, t.csat AS ticket_csat, t.tags AS ticket_tags
    FROM with_stats s
    JOIN tickets t ON t.id = s.ticket_id
)
-- Subtotals with ROLLUP
SELECT
    ticket_csat,
    actor,
    event_type_group,
    GROUPING(ticket_csat) AS is_total_ticket_csat,
    COUNT(*) AS row_count,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT event_type_group) AS distinct_event_type_groups
FROM enriched_ticket
GROUP BY ROLLUP(ticket_csat, actor, event_type_group)
