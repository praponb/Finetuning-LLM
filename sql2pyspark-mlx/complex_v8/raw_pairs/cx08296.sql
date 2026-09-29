WITH cleaned_tickets AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        channel,
        csat,
        tags
    FROM tickets
    WHERE SIZE(tags) >= 1
      AND channel = 'web'
),
enriched_agent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.tags,
        su.tier AS agent_tier
    FROM cleaned_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY MONTH(closed_at) ORDER BY csat, id) AS csat_dense_rank,
        LEAD(csat) OVER (PARTITION BY MONTH(closed_at) ORDER BY csat, id) AS next_csat,
        ROUND(csat - AVG(csat) OVER w_agent_id_agent_tier, 2) AS csat_vs_agent_id_agent_tier_avg,
        NTH_VALUE(closed_at, 2) OVER (PARTITION BY MONTH(closed_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        MIN(csat) OVER (PARTITION BY MONTH(closed_at) ORDER BY csat, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_csat_so_far
    FROM enriched_agent
    WINDOW w_agent_id_agent_tier AS (PARTITION BY agent_id, agent_tier)
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.csat,
        s.tags,
        s.agent_tier,
        s.csat_dense_rank,
        s.next_csat,
        s.min_csat_so_far,
        su.agent_name
    FROM ranked s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND LOWER(su.agent_name) LIKE '%to%'
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    csat,
    tags,
    agent_tier,
    csat_dense_rank,
    next_csat,
    min_csat_so_far,
    agent_name,
    next_csat + csat AS next_csat_plus_csat
FROM joined
ORDER BY agent_name DESC, min_csat_so_far NULLS LAST
