WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label,
        TO_DATE(event_at) AS event_at_date
    FROM ticket_events
    WHERE actor = 'customer'
      AND event_type = 'resolved'
),
combined_details AS (
    -- Attach ticket attributes
    SELECT s.*, t.agent_id AS ticket_agent_id
    FROM base s
    LEFT JOIN tickets t ON t.id = s.ticket_id
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(' / ', event_at_label, actor) AS event_at_label_actor_label
    FROM combined_details
)
SELECT
    actor,
    ticket_agent_id,
    COUNT(*) FILTER (WHERE actor = 'system') AS n_actor_system,
    ARRAY_SORT(COLLECT_SET(event_type)) AS event_type_list
FROM annotated
GROUP BY ROLLUP(actor, ticket_agent_id)
ORDER BY actor NULLS LAST, ticket_agent_id NULLS LAST
