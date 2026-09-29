WITH cleaned_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        priority,
        csat,
        tags,
        CASE WHEN priority IN ('P4', 'P1') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
    FROM tickets
    WHERE requester_id <= 22
),
with_details_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.csat,
        su.tier AS agent_tier,
        su.agent_name,
        su.team AS agent_team
    FROM cleaned_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(csat - AVG(csat) OVER (PARTITION BY csat, agent_id), 2) AS csat_vs_csat_agent_id_avg,
        LEAD(csat) OVER (PARTITION BY agent_id ORDER BY csat, id) AS next_csat,
        SUM(csat) OVER (PARTITION BY csat, agent_team ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, agent_team ORDER BY csat), 3) AS csat_cume_dist,
        ROUND((UNIX_TIMESTAMP(opened_at) - UNIX_TIMESTAMP(LAG(opened_at) OVER (PARTITION BY agent_id ORDER BY csat, id))) / 60, 1) AS minutes_since_prev_opened_at,
        SUM(csat) OVER (PARTITION BY agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_2,
        SIZE(COLLECT_SET(agent_tier) OVER w_csat_agent_id) AS distinct_agent_tier_in_csat_agent_id,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_id ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at
    FROM with_details_agent
    WINDOW w_csat_agent_id AS (PARTITION BY csat, agent_id)
),
labeled AS (
    -- Label and score rows
    SELECT
        id,
        opened_at,
        csat,
        agent_name,
        csat_vs_csat_agent_id_avg,
        next_csat,
        running_csat,
        minutes_since_prev_opened_at,
        distinct_agent_tier_in_csat_agent_id,
        first_known_opened_at,
        SUBSTRING(TRIM(agent_team), 1, 3) AS agent_team_prefix
    FROM metrics
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(csat) OVER (PARTITION BY csat, next_csat ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_so_far,
        MAX(csat_vs_csat_agent_id_avg) OVER w_csat AS csat_max_csat_vs_csat_agent_id_avg,
        ROUND(CUME_DIST() OVER (ORDER BY opened_at DESC NULLS FIRST), 3) AS opened_at_cume_dist,
        SUM(csat_vs_csat_agent_id_avg) OVER (PARTITION BY csat, next_csat ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_vs_csat_agent_id_avg,
        MAX(csat_vs_csat_agent_id_avg) OVER (PARTITION BY csat, next_csat ORDER BY csat NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_csat_vs_csat_agent_id_avg_so_far
    FROM labeled
    WINDOW w_csat AS (PARTITION BY csat)
)
-- Final projection
SELECT
    id,
    csat,
    agent_name,
    csat_vs_csat_agent_id_avg,
    next_csat,
    running_csat,
    minutes_since_prev_opened_at,
    distinct_agent_tier_in_csat_agent_id,
    first_known_opened_at,
    agent_team_prefix,
    max_csat_so_far,
    running_csat_vs_csat_agent_id_avg,
    max_csat_vs_csat_agent_id_avg_so_far,
    csat + next_csat AS csat_plus_next_csat
FROM with_windows
ORDER BY running_csat DESC
