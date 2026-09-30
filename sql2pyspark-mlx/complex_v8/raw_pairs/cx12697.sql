WITH prepared_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
),
with_parent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        su.tier AS agent_tier
    FROM prepared_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.tier = 'T2'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(csat / NULLIF(SUM(csat) OVER w_priority_agent_tier, 0), 4) AS csat_share_of_priority_agent_tier,
        SUM(csat) OVER w_priority_agent_tier AS priority_agent_tier_sum_csat,
        ROUND(AVG(csat) OVER w_closed_at_year_agent_id, 2) AS closed_at_year_agent_id_avg_csat,
        LEAD(csat, 1, 0) OVER w_csat_priority_by_closed_at AS next_csat,
        ROUND(AVG(requester_id) OVER (PARTITION BY csat, priority ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS requester_id_avg_3_prior,
        MAX(csat) OVER (ORDER BY closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS csat_max_7,
        RANK() OVER w_priority_closed_at_to_date_b AS csat_rank,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY csat, priority ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY YEAR(closed_at), agent_id), 0), 4) AS csat_share_of_closed_at_year_agent_id,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY YEAR(closed_at), agent_id)) AS distinct_agent_tier_in_closed_at_year_agent_id,
        MAX(requester_id) OVER (PARTITION BY csat, priority ORDER BY csat, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS requester_id_max_7
    FROM with_parent
    WINDOW w_priority_agent_tier AS (PARTITION BY priority, agent_tier), w_closed_at_year_agent_id AS (PARTITION BY YEAR(closed_at), agent_id), w_csat_priority_by_closed_at AS (PARTITION BY csat, priority ORDER BY closed_at DESC, id), w_priority_closed_at_to_date_b AS (PARTITION BY priority, TO_DATE(closed_at) ORDER BY csat)
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        csat,
        agent_tier,
        csat_share_of_priority_agent_tier,
        priority_agent_tier_sum_csat,
        closed_at_year_agent_id_avg_csat,
        next_csat,
        csat_max_7,
        csat_rank,
        second_closed_at,
        csat_share_of_closed_at_year_agent_id,
        distinct_agent_tier_in_closed_at_year_agent_id,
        requester_id_max_7,
        HOUR(closed_at) AS closed_at_hour,
        SUBSTRING(TRIM(priority), 1, 4) AS priority_prefix,
        LENGTH(TRIM(priority)) AS priority_len
    FROM metrics
)
-- Final projection
SELECT
    id,
    requester_id,
    priority,
    csat,
    agent_tier,
    priority_agent_tier_sum_csat,
    closed_at_year_agent_id_avg_csat,
    next_csat,
    csat_max_7,
    csat_rank,
    second_closed_at,
    distinct_agent_tier_in_closed_at_year_agent_id,
    requester_id_max_7,
    closed_at_hour,
    priority_prefix,
    priority_len
FROM tagged
ORDER BY id DESC NULLS FIRST, priority_len NULLS LAST
