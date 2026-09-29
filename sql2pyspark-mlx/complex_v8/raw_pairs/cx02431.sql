WITH scoped_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        channel,
        csat,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM tickets
    WHERE closed_at < TIMESTAMP '2025-02-22 12:00:00'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        s.priority_prefix,
        su.hired_on AS agent_hired_on,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM scoped_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        agent_tier,
        agent_team,
        INITCAP(TRIM(agent_team)) AS agent_team_clean
    FROM joined
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(csat) OVER w_agent_team_csat AS agent_team_csat_min_csat,
        ROW_NUMBER() OVER w_all_by_csat AS rn,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat DESC), 3) AS csat_percent_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY csat DESC, id), 3) AS csat_percent_rank_2,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at
    FROM annotated
    WINDOW w_agent_team_csat AS (PARTITION BY agent_team, csat), w_all_by_csat AS (ORDER BY csat DESC, id)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    csat,
    MIN(last_known_opened_at) AS first_last_known_opened_at
FROM metrics
GROUP BY ROLLUP(agent_tier, csat)
