WITH scoped AS (
    -- Filter ticket_events
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown,
        CONCAT_WS(' / ', event_type, actor) AS event_type_actor_label
    FROM ticket_events
    WHERE ticket_id >= 37
      AND event_type = 'reopened'
),
with_details_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.actor,
        s.event_type_actor_label,
        t.requester_id AS ticket_requester_id,
        t.agent_id AS ticket_agent_id,
        su.hired_on AS agent_hired_on
    FROM scoped s
    JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        DATE_FORMAT(agent_hired_on, 'yyyy-MM-dd') AS agent_hired_on_label,
        CASE WHEN actor IN ('agent', 'system') THEN 'group_a' WHEN actor = 'customer' THEN 'group_b' ELSE 'other' END AS actor_group,
        DATE_TRUNC('QUARTER', agent_hired_on) AS agent_hired_on_quarter
    FROM with_details_ticket
)
-- Final projection
SELECT
    id,
    actor,
    event_type_actor_label,
    ticket_agent_id,
    agent_hired_on,
    agent_hired_on_label,
    actor_group,
    agent_hired_on_quarter,
    DATEDIFF(DATE '2026-01-01', agent_hired_on) AS days_since_agent_hired_on,
    YEAR(agent_hired_on) AS agent_hired_on_year
FROM scored
ORDER BY days_since_agent_hired_on DESC
