WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        DATE_FORMAT(event_at, 'yyyy-MM-dd HH:00') AS event_at_label,
        CASE WHEN event_type IN ('created', 'assigned') THEN 'group_a' WHEN event_type = 'replied' THEN 'group_b' ELSE 'other' END AS event_type_group,
        CASE WHEN actor IN ('customer', 'agent') THEN 'group_a' WHEN actor = 'system' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
joined_ticket AS (
    -- Join tickets
    SELECT
        s.*,
        t.tags AS ticket_tags,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM valid s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_at,
        event_type,
        actor,
        event_type_group,
        actor_group,
        ticket_tags,
        agent_hired_on,
        agent_name,
        LEAD(agent_hired_on, 3) OVER (ORDER BY event_at DESC, id) AS next3_agent_hired_on,
        LAG(agent_hired_on, 2) OVER w_all_by_event_at AS prev2_agent_hired_on
    FROM joined_ticket
    WINDOW w_all_by_event_at AS (ORDER BY event_at DESC, id)
)
SELECT
    id,
    event_at,
    event_type,
    event_type_group,
    actor_group,
    ticket_tags,
    agent_hired_on,
    agent_name,
    prev2_agent_hired_on,
    SUBSTRING(TRIM(event_type_group), 1, 3) AS event_type_group_prefix
FROM sequenced
WHERE prev2_agent_hired_on < DATE '2024-12-31'
ORDER BY agent_hired_on DESC NULLS FIRST, agent_name
