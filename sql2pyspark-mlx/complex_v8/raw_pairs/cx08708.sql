WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT_WS('|', actor, event_type) AS actor_event_type_label,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE event_type NOT IN ('resolved', 'replied')
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN actor_or_unknown IN ('customer', 'UNKNOWN') THEN 'group_a' WHEN actor_or_unknown = 'system' THEN 'group_b' ELSE 'other' END AS actor_or_unknown_group,
        CONCAT_WS('-', actor_event_type_label, event_type) AS actor_event_type_label_event_type_label,
        LOWER(TRIM(actor)) AS actor_clean
    FROM recent
    WHERE HOUR(event_at) BETWEEN 8 AND 13
),
combined_details_ticket AS (
    -- Enrich with dimension data
    SELECT s.*, t.requester_id AS ticket_requester_id, su.hired_on AS agent_hired_on
    FROM tagged s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
    WHERE t.requester_id < 18
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    actor,
    actor_event_type_label,
    actor_or_unknown,
    actor_event_type_label_event_type_label,
    actor_clean,
    ticket_requester_id,
    DATEDIFF(DATE '2026-01-01', agent_hired_on) AS days_since_agent_hired_on
FROM combined_details_ticket
ORDER BY actor, actor_or_unknown NULLS LAST
