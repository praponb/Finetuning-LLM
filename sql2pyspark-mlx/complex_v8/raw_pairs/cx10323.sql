WITH filtered AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        tags,
        DATEDIFF(DATE '2025-06-30', opened_at) AS days_since_opened_at,
        CONCAT(channel, '#', CAST(requester_id AS STRING)) AS channel_ref
    FROM tickets
    WHERE requester_id <= 22
      AND TO_DATE(closed_at) < DATE '2025-03-22'
),
combined_details AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.closed_at,
        s.priority,
        s.csat,
        s.channel_ref,
        su.agent_name,
        su.team AS agent_team,
        su.hired_on AS agent_hired_on
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE su.hired_on IS NOT NULL
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        csat,
        channel_ref,
        agent_team,
        MAX(csat) OVER (PARTITION BY agent_id, TO_DATE(closed_at) ORDER BY opened_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS csat_max_4,
        SUM(csat) OVER (PARTITION BY agent_id, TO_DATE(closed_at) ORDER BY opened_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS csat_sum_4,
        RANK() OVER w_all_by_closed_at_ties AS closed_at_rank,
        FIRST_VALUE(opened_at) IGNORE NULLS OVER (PARTITION BY agent_team ORDER BY closed_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_opened_at,
        ROUND(AVG(csat) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_2_prior,
        MIN(csat) OVER w_priority_csat AS priority_csat_min_csat,
        ROUND(PERCENT_RANK() OVER (ORDER BY closed_at DESC), 3) AS closed_at_percent_rank,
        LEAD(csat) OVER (PARTITION BY agent_id, TO_DATE(closed_at) ORDER BY opened_at, id) AS next_csat,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id, TO_DATE(closed_at) ORDER BY opened_at, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS csat_avg_2,
        MAX(csat) OVER (PARTITION BY priority, YEAR(opened_at) ORDER BY agent_hired_on DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_max_1_prior,
        ROUND(AVG(csat) OVER (PARTITION BY agent_id, TO_DATE(closed_at) ORDER BY opened_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_avg_1_prior,
        MAX(csat) OVER (PARTITION BY priority, csat) AS priority_csat_max_csat,
        SIZE(COLLECT_SET(agent_team) OVER (PARTITION BY priority, csat)) AS distinct_agent_team_in_priority_csat,
        LEAD(agent_hired_on, 3) OVER w_priority_opened_at_year_by_a_2 AS next3_agent_hired_on
    FROM combined_details
    WINDOW w_all_by_closed_at_ties AS (ORDER BY closed_at DESC), w_priority_csat AS (PARTITION BY priority, csat), w_priority_opened_at_year_by_a_2 AS (PARTITION BY priority, YEAR(opened_at) ORDER BY agent_hired_on DESC, id)
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        channel_ref,
        agent_team,
        csat_sum_4,
        closed_at_rank,
        closed_at_percent_rank,
        csat_avg_2,
        csat_max_1_prior,
        csat_avg_1_prior,
        priority_csat_max_csat,
        distinct_agent_team_in_priority_csat,
        next3_agent_hired_on,
        ABS(csat_avg_2 - 3.8) AS csat_avg_2_distance
    FROM sequenced
    WHERE distinct_agent_team_in_priority_csat > 5
),
sequenced_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY closed_at_rank, priority ORDER BY csat), 3) AS csat_percent_rank,
        SUM(csat_avg_2_distance) OVER (PARTITION BY csat) AS csat_sum_csat_avg_2_distance,
        ROW_NUMBER() OVER (ORDER BY distinct_agent_team_in_priority_csat ASC, id) AS seq_no,
        DENSE_RANK() OVER w_closed_at_rank_priority_by_c AS csat_dense_rank,
        ROUND(csat_max_1_prior - AVG(csat_max_1_prior) OVER w_csat, 2) AS csat_max_1_prior_vs_csat_avg,
        ROUND(PERCENT_RANK() OVER (ORDER BY distinct_agent_team_in_priority_csat ASC), 3) AS distinct_agent_team_in_priority_csat_percent_ran,
        LEAD(csat_sum_4) OVER (PARTITION BY closed_at_rank, priority ORDER BY csat, id) AS next_csat_sum_4
    FROM tagged
    WINDOW w_closed_at_rank_priority_by_c AS (PARTITION BY closed_at_rank, priority ORDER BY csat), w_csat AS (PARTITION BY csat)
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    channel_ref,
    agent_team,
    closed_at_rank,
    closed_at_percent_rank,
    csat_avg_2,
    csat_max_1_prior,
    csat_avg_1_prior,
    priority_csat_max_csat,
    distinct_agent_team_in_priority_csat,
    next3_agent_hired_on,
    csat_avg_2_distance,
    csat_percent_rank,
    csat_sum_csat_avg_2_distance,
    csat_max_1_prior_vs_csat_avg,
    distinct_agent_team_in_priority_csat_percent_ran,
    FLOOR(csat_sum_4 / 2) * 2 AS csat_sum_4_bucket
FROM sequenced_2
WHERE HOUR(closed_at) BETWEEN 8 AND 14
ORDER BY channel_ref, distinct_agent_team_in_priority_csat, id
LIMIT 5
