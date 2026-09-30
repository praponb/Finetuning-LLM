WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(channel), 1, 4) AS channel_prefix,
        ROUND(csat / NULLIF(requester_id, 0), 3) AS csat_per_requester_id
    FROM tickets
    WHERE (requester_id BETWEEN 7 AND 25 OR TO_DATE(closed_at) < DATE '2025-02-10')
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        channel_prefix,
        csat_per_requester_id,
        INITCAP(TRIM(channel)) AS channel_clean,
        FLOOR(csat_per_requester_id / 2) * 2 AS csat_per_requester_id_bucket
    FROM valid
    WHERE csat > 2
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.requester_id,
        s.agent_id,
        s.closed_at,
        s.tags,
        s.channel_prefix,
        s.channel_clean,
        s.csat_per_requester_id_bucket,
        su.tier AS agent_tier
    FROM scored s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_tier,
    agent_id,
    csat_per_requester_id_bucket,
    SUM(CASE WHEN agent_tier IN ('T1', 'T2') THEN csat_per_requester_id_bucket ELSE 0 END) AS csat_per_requester_id_bucket_agent_tier_in_t1_t2,
    MAX(closed_at) AS last_closed_at,
    ROUND(STDDEV(requester_id), 2) AS stddev_requester_id
FROM enriched
GROUP BY GROUPING SETS ((agent_tier, agent_id, csat_per_requester_id_bucket), (agent_tier), (agent_id), ())
ORDER BY agent_tier NULLS LAST, agent_id NULLS LAST, csat_per_requester_id_bucket NULLS LAST
