WITH recent_tickets AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(priority), 1, 2) AS priority_prefix
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND closed_at < TIMESTAMP '2025-02-26 16:00:00'
),
joined_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.priority,
        s.csat,
        s.tags,
        s.priority_prefix,
        su.tier AS agent_tier
    FROM recent_tickets s
    JOIN support_agents su ON su.id = s.agent_id
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(csat) OVER w_all_by_opened_at AS next_csat,
        FIRST_VALUE(csat) OVER (ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_csat,
        LAST_VALUE(opened_at) IGNORE NULLS OVER (ORDER BY opened_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_opened_at,
        SUM(csat) OVER (ORDER BY opened_at ASC NULLS LAST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS csat_sum_3,
        ROUND(csat / NULLIF(SUM(csat) OVER (PARTITION BY priority, csat), 0), 4) AS csat_share_of_priority_csat,
        MAX(csat) OVER (ORDER BY opened_at DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS csat_max_1_prior,
        FIRST_VALUE(opened_at) OVER (ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_opened_at,
        MAX(csat) OVER w_priority_csat AS priority_csat_max_csat,
        SIZE(COLLECT_SET(agent_tier) OVER w_priority_csat) AS distinct_agent_tier_in_priority_csat,
        SUM(csat) OVER (PARTITION BY priority, csat) AS priority_csat_sum_csat,
        LAST_VALUE(opened_at) OVER (ORDER BY csat ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_opened_at,
        SUM(csat) OVER (PARTITION BY priority, agent_tier) AS priority_agent_tier_sum_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY priority, agent_tier)) AS distinct_agent_id_in_priority_agent_tier
    FROM joined_agent
    WINDOW w_all_by_opened_at AS (ORDER BY opened_at DESC, id), w_priority_csat AS (PARTITION BY priority, csat)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(csat, 1, 0) OVER (PARTITION BY csat, distinct_agent_tier_in_priority_csat ORDER BY distinct_agent_tier_in_priority_csat, id) AS next_csat_2,
        LEAD(csat_share_of_priority_csat) OVER (PARTITION BY csat, distinct_agent_tier_in_priority_csat ORDER BY distinct_agent_tier_in_priority_csat, id) AS next_csat_share_of_priority_csat,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY distinct_agent_tier_in_priority_csat)) AS distinct_agent_id_in_distinct_agent_tier_in_prio,
        ROUND(PERCENT_RANK() OVER w_all_by_priority_csat_max_csa, 3) AS priority_csat_max_csat_percent_rank,
        FIRST_VALUE(next_csat) IGNORE NULLS OVER (PARTITION BY csat, distinct_agent_tier_in_priority_csat ORDER BY distinct_agent_tier_in_priority_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_csat,
        NTILE(5) OVER (ORDER BY priority_csat_max_csat DESC NULLS FIRST, id) AS priority_csat_max_csat_quintile,
        SUM(csat) OVER (PARTITION BY csat, distinct_agent_tier_in_priority_csat ORDER BY distinct_agent_tier_in_priority_csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat
    FROM metrics
    WINDOW w_all_by_priority_csat_max_csa AS (ORDER BY priority_csat_max_csat DESC NULLS FIRST, id)
)
-- Final projection
SELECT
    id,
    agent_id,
    opened_at,
    priority,
    csat,
    tags,
    priority_prefix,
    agent_tier,
    first_csat,
    last_known_opened_at,
    csat_sum_3,
    csat_share_of_priority_csat,
    csat_max_1_prior,
    first_opened_at,
    priority_csat_max_csat,
    priority_csat_sum_csat,
    distinct_agent_id_in_priority_agent_tier,
    next_csat_2,
    next_csat_share_of_priority_csat,
    distinct_agent_id_in_distinct_agent_tier_in_prio,
    priority_csat_max_csat_percent_rank,
    first_known_next_csat,
    running_csat,
    CONCAT(agent_tier, '#', CAST(running_csat AS STRING)) AS agent_tier_ref
FROM sequenced
WHERE distinct_agent_id_in_priority_agent_tier > 2
ORDER BY id, first_csat DESC
LIMIT 20
