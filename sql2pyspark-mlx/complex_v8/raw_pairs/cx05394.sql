WITH valid AS (
    -- Keep valid rows and derive helper columns
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
    WHERE channel = 'email'
),
joined AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.channel,
        s.csat,
        s.tags,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY priority, MONTH(opened_at) ORDER BY closed_at, id) AS closed_at_tercile,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY priority, MONTH(opened_at) ORDER BY closed_at, id))) / 60, 1) AS minutes_since_prev_opened_at,
        COUNT(*) OVER (PARTITION BY agent_team, channel) AS rows_in_agent_team_channel,
        MAX(csat) OVER (PARTITION BY priority, MONTH(opened_at) ORDER BY closed_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS csat_max_6_prior,
        MAX(csat) OVER (PARTITION BY priority, MONTH(opened_at) ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far
    FROM joined
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        agent_team,
        closed_at_tercile,
        minutes_since_prev_opened_at,
        csat_max_6_prior,
        LEAD(closed_at_tercile) OVER (PARTITION BY channel, agent_tier ORDER BY csat, id) AS next_closed_at_tercile,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY channel)) AS distinct_agent_tier_in_channel,
        FIRST_VALUE(minutes_since_prev_opened_at) IGNORE NULLS OVER (PARTITION BY channel, agent_tier ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_minutes_since_prev_opened_at,
        LEAD(csat_max_6_prior, 2) OVER w_channel_agent_tier_by_csat AS next2_csat_max_6_prior,
        ROUND(max_csat_so_far - AVG(max_csat_so_far) OVER (PARTITION BY channel), 2) AS max_csat_so_far_vs_channel_avg
    FROM metrics
    WINDOW w_channel_agent_tier_by_csat AS (PARTITION BY channel, agent_tier ORDER BY csat, id)
)
SELECT
    priority,
    agent_id,
    agent_team,
    GROUPING_ID() AS gid,
    SUM(CASE WHEN priority IN ('P2', 'P1', 'P4') THEN 1 ELSE 0 END) AS n_priority_in_p2_p1_p4
FROM ranked
GROUP BY CUBE(priority, agent_id, agent_team)
