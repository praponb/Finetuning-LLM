WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat
    FROM tickets
    WHERE (HOUR(opened_at) BETWEEN 9 AND 13 OR requester_id < 22)
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM prepared
    WHERE requester_id > (SELECT AVG(requester_id) FROM prepared)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        su.team AS agent_team
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.agent_team,
        su.agent_name
    FROM with_parent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
joined AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.team AS agent_team_2,
        su.agent_name AS agent_agent_name,
        su.tier AS agent_tier
    FROM joined_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
latest AS (
    -- Keep the earliest row per agent_tier, priority
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        agent_team,
        agent_name,
        agent_team_2,
        agent_agent_name,
        agent_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_tier, priority ORDER BY opened_at, id) AS rn
        FROM joined
    ) ranked
    WHERE rn = 1
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(agent_team_2, 'UNKNOWN') AS agent_team_2_or_unknown,
        LENGTH(TRIM(priority)) AS priority_len,
        opened_at + INTERVAL 2 HOURS AS opened_at_plus_2h
    FROM latest
)
-- Final projection
SELECT
    id,
    opened_at,
    priority,
    agent_team,
    agent_name,
    agent_team_2,
    agent_agent_name,
    agent_tier,
    opened_at_plus_2h
FROM derived
ORDER BY agent_name
