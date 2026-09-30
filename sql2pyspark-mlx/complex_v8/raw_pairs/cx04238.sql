WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        DATE_TRUNC('DAY', opened_at) AS opened_at_day,
        requester_id - csat AS requester_id_minus_csat
    FROM tickets
    WHERE channel IS NOT NULL
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.priority,
        s.tags,
        s.opened_at_day,
        s.requester_id_minus_csat,
        su.agent_name,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        priority,
        tags,
        opened_at_day,
        requester_id_minus_csat,
        agent_name,
        DAYOFWEEK(agent_hired_on) AS agent_hired_on_dayofweek
    FROM with_details_agent
    WHERE requester_id <= 23
),
screened AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM annotated s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND t.channel = 'web'
              AND t.csat < 5
        )
      AND HOUR(s.opened_at_day) BETWEEN 7 AND 12
)
SELECT *
FROM (
    SELECT agent_id, priority, agent_hired_on_dayofweek
    FROM screened
) src
PIVOT (
    SUM(agent_hired_on_dayofweek)
    FOR priority IN ('P1', 'P3', 'P4', 'P2')
)
ORDER BY agent_id
