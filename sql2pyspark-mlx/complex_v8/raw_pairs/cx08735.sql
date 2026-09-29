WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE opened_at >= TIMESTAMP '2025-01-31 08:00:00'
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.tags,
        su.tier AS agent_tier,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM recent s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY priority, agent_team ORDER BY agent_hired_on DESC) AS agent_hired_on_rank,
        ROUND(CUME_DIST() OVER w_priority_agent_team_by_agent_2, 3) AS agent_hired_on_cume_dist,
        ROW_NUMBER() OVER (ORDER BY agent_hired_on, id) AS seq_no,
        RANK() OVER (ORDER BY agent_hired_on) AS agent_hired_on_rank_2,
        LEAD(opened_at) OVER w_priority_agent_team_by_agent_2 AS next_opened_at,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        DATEDIFF(agent_hired_on, LAG(agent_hired_on) OVER (ORDER BY agent_hired_on, id)) AS days_since_prev_agent_hired_on,
        NTILE(5) OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY opened_at ASC NULLS LAST, id) AS opened_at_quintile,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY agent_hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        NTILE(10) OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY opened_at ASC NULLS LAST, id) AS opened_at_decile,
        NTILE(10) OVER (PARTITION BY priority, agent_team ORDER BY agent_hired_on DESC, id) AS agent_hired_on_decile,
        RANK() OVER (PARTITION BY YEAR(opened_at), agent_id ORDER BY opened_at ASC NULLS LAST, id) AS opened_at_rank
    FROM joined
    WINDOW w_priority_agent_team_by_agent_2 AS (PARTITION BY priority, agent_team ORDER BY agent_hired_on DESC, id), w_all_by_agent_hired_on AS (ORDER BY agent_hired_on, id)
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(agent_hired_on_rank_2 - AVG(agent_hired_on_rank_2) OVER (PARTITION BY agent_hired_on_rank), 2) AS agent_hired_on_rank_2_vs_agent_hired_on_rank_avg,
        LAST_VALUE(opened_at_quintile) OVER (PARTITION BY priority ORDER BY agent_hired_on_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at_quintile,
        MAX(agent_hired_on_cume_dist) OVER (PARTITION BY agent_hired_on_rank, priority ORDER BY opened_at_quintile, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS agent_hired_on_cume_dist_max_7,
        opened_at_quintile - LAG(opened_at_quintile) OVER w_agent_hired_on_rank_priority_2 AS opened_at_quintile_change,
        RANK() OVER w_first_known_opened_at_month AS next_opened_at_rank
    FROM windowed
    WINDOW w_agent_hired_on_rank_priority_2 AS (PARTITION BY agent_hired_on_rank, priority ORDER BY opened_at_quintile, id), w_first_known_opened_at_month AS (PARTITION BY MONTH(first_known_opened_at), agent_hired_on_rank_2 ORDER BY next_opened_at DESC)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    tags,
    agent_hired_on,
    agent_hired_on_rank,
    days_since_prev_agent_hired_on,
    opened_at_quintile,
    last_known_opened_at,
    opened_at_decile,
    opened_at_rank,
    last_opened_at_quintile,
    agent_hired_on_cume_dist_max_7,
    opened_at_quintile_change
FROM with_windows
ORDER BY priority
