WITH base AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags
    FROM tickets
    WHERE requester_id < 19
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        priority,
        tags,
        SUM(csat) OVER (PARTITION BY YEAR(closed_at) ORDER BY opened_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_id), 2) AS csat_vs_agent_id_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY YEAR(closed_at) ORDER BY opened_at ASC), 3) AS opened_at_percent_rank
    FROM base
),
unique_rows AS (
    -- Keep the latest row per agent_id, priority
    SELECT
        id,
        agent_id,
        priority,
        tags,
        running_csat,
        csat_vs_agent_id_avg,
        opened_at_percent_rank
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY agent_id, priority ORDER BY running_csat DESC, id DESC) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.tags,
        s.running_csat,
        s.opened_at_percent_rank,
        su.team AS agent_team,
        su.agent_name,
        su.tier AS agent_tier
    FROM unique_rows s
    JOIN support_agents su ON su.id = s.agent_id AND su.team <> 'Onboarding'
)
SELECT
    COALESCE(priority, 'ALL') AS priority,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    ROUND(STDDEV(running_csat), 2) AS stddev_running_csat,
    ROUND(AVG(opened_at_percent_rank), 2) AS avg_opened_at_percent_rank,
    COUNT(*) AS cnt
FROM joined
GROUP BY CUBE(priority, agent_tier)
ORDER BY priority NULLS LAST, agent_tier NULLS LAST
