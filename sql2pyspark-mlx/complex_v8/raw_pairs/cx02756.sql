WITH filtered AS (
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
        tags
    FROM tickets
    WHERE (SIZE(tags) >= 1 OR priority IN ('P2', 'P3', 'P1'))
),
latest_per_key AS (
    -- Keep the latest row per agent_id
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY opened_at DESC, id DESC) AS row_num
        FROM filtered
    ) ranked
    WHERE row_num = 1
),
joined_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.tags,
        su.team AS agent_team,
        su.agent_name,
        su.tier AS agent_tier
    FROM latest_per_key s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
joined_agent_2 AS (
    -- Attach agent attributes
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.channel,
        s.csat,
        s.agent_team,
        s.agent_name,
        s.agent_tier,
        su.hired_on AS agent_hired_on,
        su.team AS agent_team_2
    FROM joined_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with ROLLUP
SELECT
    csat,
    COALESCE(agent_team_2, 'ALL') AS agent_team_2,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    GROUPING(csat) AS is_total_csat,
    ROUND(SUM(requester_id) / NULLIF(SUM(csat), 0), 3) AS requester_id_to_csat_ratio,
    ROUND(100 * COUNT(CASE WHEN channel = 'email' THEN 1 END) / COUNT(*), 1) AS pct_channel_email,
    SUM(requester_id) AS total_requester_id
FROM joined_agent_2
GROUP BY ROLLUP(csat, agent_team_2, agent_tier)
