WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        LENGTH(TRIM(channel)) AS channel_len
    FROM tickets
    WHERE HOUR(opened_at) BETWEEN 9 AND 14
),
with_parent_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        su.team AS agent_team,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM scoped s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on BETWEEN DATE '2024-05-02' AND DATE '2025-03-30'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        agent_team,
        agent_name,
        agent_hired_on,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, csat ORDER BY closed_at), 3) AS closed_at_cume_dist,
        RANK() OVER (PARTITION BY csat, agent_id ORDER BY closed_at DESC NULLS FIRST, id) AS closed_at_rank,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY agent_team, csat ORDER BY agent_hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY csat, priority)) AS distinct_agent_team_in_csat_priority,
        SUM(csat) OVER (PARTITION BY csat, agent_team) AS csat_agent_team_sum_csat,
        COUNT(*) OVER w_csat_priority AS rows_in_csat_priority,
        MAX(csat) OVER (PARTITION BY agent_id, csat ORDER BY closed_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_max_1_prior,
        LAG(csat) OVER (PARTITION BY agent_team, csat ORDER BY agent_hired_on DESC, id) AS prev_csat
    FROM with_parent_agent
    WINDOW w_csat_priority AS (PARTITION BY csat, priority)
),
labeled AS (
    -- Derive helper columns
    SELECT *, TO_DATE(second_closed_at) AS second_closed_at_date
    FROM sequenced
    WHERE rows_in_csat_priority > 9
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(closed_at_rank) OVER (ORDER BY csat_agent_team_sum_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_closed_at_rank,
        LEAD(closed_at) OVER w_all_by_csat_agent_team_sum_c_2 AS next_closed_at,
        LAG(closed_at_rank) OVER w_agent_id_prev_csat_by_distin AS prev_closed_at_rank
    FROM labeled
    WINDOW w_all_by_csat_agent_team_sum_c_2 AS (ORDER BY csat_agent_team_sum_csat, id), w_agent_id_prev_csat_by_distin AS (PARTITION BY agent_id, prev_csat ORDER BY distinct_agent_team_in_csat_priority, id)
)
-- Final projection
SELECT
    id,
    priority,
    agent_team,
    agent_name,
    agent_hired_on,
    closed_at_cume_dist,
    closed_at_rank,
    csat_agent_team_sum_csat,
    rows_in_csat_priority,
    csat_max_1_prior,
    prev_csat,
    last_closed_at_rank,
    next_closed_at,
    prev_closed_at_rank
FROM metrics
