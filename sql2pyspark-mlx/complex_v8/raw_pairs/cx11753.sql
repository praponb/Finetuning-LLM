WITH prepared AS (
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
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'upgrade')
),
combined_details_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.tags,
        s.least_csat_requester_id,
        su.tier AS agent_tier,
        su.agent_name,
        su.hired_on AS agent_hired_on
    FROM prepared s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        agent_id,
        priority,
        csat,
        tags,
        least_csat_requester_id,
        agent_name,
        agent_hired_on,
        SUM(least_csat_requester_id) OVER (PARTITION BY agent_id, csat ORDER BY csat DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS least_csat_requester_id_sum_2,
        RANK() OVER (PARTITION BY agent_id, csat ORDER BY csat DESC) AS csat_rank,
        LEAD(least_csat_requester_id, 2) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS next2_least_csat_requester_id,
        SUM(csat) OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS csat_sum_6_prior,
        LEAD(closed_at, 2) OVER (PARTITION BY agent_id ORDER BY closed_at, id) AS next2_closed_at,
        ROUND(AVG(least_csat_requester_id) OVER (PARTITION BY agent_tier), 2) AS agent_tier_avg_least_csat_requester_id,
        DENSE_RANK() OVER (PARTITION BY agent_tier ORDER BY closed_at DESC) AS closed_at_dense_rank,
        LAG(least_csat_requester_id) OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id) AS prev_least_csat_requester_id,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_tier), 2) AS csat_vs_agent_tier_avg,
        SUM(csat) OVER w_agent_tier AS agent_tier_sum_csat,
        MAX(csat) OVER (PARTITION BY agent_id ORDER BY closed_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS csat_max_2_prior,
        NTH_VALUE(agent_hired_on, 2) OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_agent_hired_on,
        FIRST_VALUE(closed_at) IGNORE NULLS OVER (PARTITION BY agent_id, csat ORDER BY csat DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_closed_at
    FROM combined_details_agent
    WINDOW w_agent_tier AS (PARTITION BY agent_tier)
)
-- Final projection
SELECT
    id,
    agent_id,
    priority,
    csat,
    least_csat_requester_id,
    agent_name,
    agent_hired_on,
    least_csat_requester_id_sum_2,
    next2_least_csat_requester_id,
    next2_closed_at,
    agent_tier_avg_least_csat_requester_id,
    prev_least_csat_requester_id,
    csat_vs_agent_tier_avg,
    agent_tier_sum_csat,
    csat_max_2_prior,
    second_agent_hired_on,
    first_known_closed_at,
    FLOOR(agent_tier_sum_csat / 10) * 10 AS agent_tier_sum_csat_bucket
FROM metrics
WHERE csat_sum_6_prior <= 12
ORDER BY agent_tier_sum_csat DESC NULLS FIRST, next2_closed_at DESC
