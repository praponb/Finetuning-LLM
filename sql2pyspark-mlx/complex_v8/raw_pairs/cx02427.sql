WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        LOWER(REGEXP_REPLACE(TRIM(event_type), ' ', '_')) AS event_type_slug,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE event_type <> 'escalated'
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.actor,
        s.event_type_slug,
        s.event_type_actor_label,
        t.opened_at AS ticket_opened_at,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags
    FROM valid s
    JOIN tickets t ON t.id = s.ticket_id AND t.closed_at < TIMESTAMP '2025-02-24 13:00:00'
)
-- Subtotals with CUBE
SELECT
    actor,
    ticket_agent_id,
    ticket_id,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(actor)) AS actor_list,
    SUM(CASE WHEN actor = 'customer' THEN 1 ELSE 0 END) AS n_actor_customer
FROM with_details_ticket
GROUP BY CUBE(actor, ticket_agent_id, ticket_id)
