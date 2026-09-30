WITH scoped_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags,
        TO_DATE(closed_at) AS closed_at_date
    FROM tickets
    WHERE requester_id <= 22
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        s.closed_at_date,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on BETWEEN DATE '2024-07-09' AND DATE '2025-02-27'
),
enriched_agent AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.tags,
        s.agent_hired_on,
        su.agent_name AS agent_agent_name,
        su.hired_on AS agent_hired_on_2
    FROM combined_details s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    channel,
    agent_hired_on,
    agent_agent_name,
    agent_hired_on_2,
    INITCAP(TRIM(agent_agent_name)) AS agent_agent_name_clean
FROM enriched_agent
ORDER BY id DESC NULLS FIRST, closed_at DESC NULLS FIRST
