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
        CASE WHEN requester_id >= 22 THEN 'HIGH' WHEN requester_id >= 7 THEN 'MEDIUM' ELSE 'LOW' END AS requester_id_tier,
        LEAST(csat, requester_id) AS least_csat_requester_id
    FROM tickets
    WHERE agent_id IS NOT NULL
      AND channel <> 'phone'
),
latest_per_key AS (
    -- Keep the latest row per channel, requester_id_tier
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        requester_id_tier,
        least_csat_requester_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY channel, requester_id_tier ORDER BY least_csat_requester_id DESC, id DESC) AS recency_rank
        FROM valid
    ) ranked
    WHERE recency_rank = 1
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    COALESCE(channel, 'ALL') AS channel,
    COALESCE(requester_id_tier, 'ALL') AS requester_id_tier,
    SUM(csat) AS total_csat,
    PERCENTILE_APPROX(least_csat_requester_id, 0.5) AS median_least_csat_requester_id
FROM latest_per_key
GROUP BY GROUPING SETS ((agent_id, channel, requester_id_tier), (agent_id), (channel), ())
ORDER BY agent_id NULLS LAST, channel NULLS LAST, requester_id_tier NULLS LAST
