WITH base_tickets AS (
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
        COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
    FROM tickets
    WHERE channel IS NOT NULL
),
enriched AS (
    -- Join support_agents
    SELECT s.*, su.agent_name, su.tier AS agent_tier
    FROM base_tickets s
    LEFT JOIN support_agents su ON su.id = s.agent_id AND su.hired_on IS NOT NULL
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id,
        NTH_VALUE(closed_at, 2) OVER (ORDER BY requester_id ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_closed_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY agent_id, priority_or_unknown ORDER BY csat, id), 3) AS csat_percent_rank,
        NTILE(3) OVER (ORDER BY requester_id ASC, id) AS requester_id_tercile,
        RANK() OVER (ORDER BY requester_id ASC) AS requester_id_rank,
        NTH_VALUE(requester_id, 2) OVER (PARTITION BY csat, channel ORDER BY requester_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_requester_id_2,
        ROUND(csat - AVG(csat) OVER (PARTITION BY agent_tier, agent_id), 2) AS csat_vs_agent_tier_agent_id_avg,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_tier ORDER BY closed_at DESC, id), 3) AS closed_at_cume_dist,
        LAG(closed_at) OVER w_agent_id_priority_or_unknown AS prev_closed_at,
        ROUND(csat / NULLIF(SUM(csat) OVER w_agent_id_csat, 0), 4) AS csat_share_of_agent_id_csat
    FROM enriched
    WINDOW w_agent_id_priority_or_unknown AS (PARTITION BY agent_id, priority_or_unknown ORDER BY csat, id), w_agent_id_csat AS (PARTITION BY agent_id, csat)
)
SELECT
    id,
    closed_at,
    priority,
    channel,
    csat,
    tags,
    priority_or_unknown,
    agent_name,
    agent_tier,
    second_requester_id,
    second_closed_at,
    csat_percent_rank,
    requester_id_tercile,
    csat_vs_agent_tier_agent_id_avg,
    closed_at_cume_dist,
    prev_closed_at,
    csat_share_of_agent_id_csat
FROM with_trends
WHERE SIZE(tags) >= 1
ORDER BY second_requester_id, id
LIMIT 20
