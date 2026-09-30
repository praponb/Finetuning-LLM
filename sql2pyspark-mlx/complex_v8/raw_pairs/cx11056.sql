WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        UPPER(TRIM(event_type)) AS event_type_clean,
        CASE WHEN actor IN ('agent', 'customer') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group,
        CONCAT_WS(': ', actor, event_type) AS actor_event_type_label
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_details AS (
    -- Join tickets
    SELECT
        s.id,
        s.ticket_id,
        s.event_type,
        s.event_type_clean,
        s.actor_group,
        s.actor_event_type_label,
        t.opened_at AS ticket_opened_at,
        t.requester_id AS ticket_requester_id,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM prepared s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
with_activity AS (
    -- Rows that have matching ticket_events
    SELECT s.*
    FROM with_details s
    WHERE s.ticket_id IN (
            SELECT t.ticket_id
            FROM ticket_events t
            WHERE t.actor NOT IN ('system', 'customer')
              AND t.event_type IN ('created', 'replied')
        )
)
SELECT
    id,
    event_type,
    event_type_clean,
    actor_group,
    ticket_opened_at,
    ticket_requester_id,
    agent_hired_on,
    agent_name,
    FLOOR(ticket_requester_id / 5) * 5 AS ticket_requester_id_bucket,
    DATEDIFF(DATE '2025-12-31', agent_hired_on) AS days_since_agent_hired_on
FROM with_activity
WHERE ticket_requester_id <= 21
ORDER BY ticket_opened_at DESC
