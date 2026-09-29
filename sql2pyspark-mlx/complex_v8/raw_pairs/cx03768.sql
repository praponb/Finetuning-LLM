WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd HH:00') AS opened_at_label,
        CASE WHEN csat >= 4 THEN 'large' WHEN csat >= 3 THEN 'medium' ELSE 'small' END AS csat_tier
    FROM tickets
    WHERE csat > 3
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        s.opened_at_label,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team,
        su.tier AS agent_tier
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
),
derived AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(priority)) AS priority_len
    FROM joined_agent
    WHERE agent_tier NOT IN ('T2', 'T3')
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT opened_at, priority, csat
    FROM derived
) src
PIVOT (
    MIN(csat)
    FOR priority IN ('P1', 'P4', 'P2')
)
