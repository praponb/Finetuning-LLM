WITH filtered AS (
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
        CONCAT_WS('-', channel, priority) AS channel_priority_label,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE closed_at >= TIMESTAMP '2025-01-17 21:00:00'
),
joined AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.priority,
        s.csat,
        s.tags,
        s.channel_priority_label,
        s.requester_id_bucket,
        su.agent_name,
        su.tier AS agent_tier
    FROM filtered s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LENGTH(TRIM(su.agent_name)) >= 7
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        agent_tier,
        agent_id,
        csat,
        ROUND(SUM(csat) / NULLIF(SUM(requester_id_bucket), 0), 3) AS csat_to_requester_id_bucket_ratio,
        ARRAY_SORT(COLLECT_SET(priority)) AS priority_list,
        SUM(requester_id_bucket) AS total_requester_id_bucket,
        ROUND(AVG(csat), 2) AS avg_csat,
        COUNT(DISTINCT agent_tier) AS distinct_agent_tiers,
        MIN(csat) AS min_csat
    FROM joined
    GROUP BY agent_tier, agent_id, csat
),
with_activity AS (
    -- Rows that have matching tickets
    SELECT s.*
    FROM totals s
    WHERE EXISTS (
            SELECT 1
            FROM tickets t
            WHERE t.agent_id = s.agent_id
              AND TO_DATE(t.opened_at) < DATE '2025-02-12'
              AND t.requester_id BETWEEN 2 AND 23
        )
),
flattened AS (
    -- One row per priority_list_item
    SELECT
        s.agent_tier,
        s.agent_id,
        s.csat,
        s.csat_to_requester_id_bucket_ratio,
        s.total_requester_id_bucket,
        s.avg_csat,
        s.distinct_agent_tiers,
        s.min_csat,
        t.priority_list_item
    FROM with_activity s
    LATERAL VIEW EXPLODE(s.priority_list) t AS priority_list_item
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        agent_tier,
        agent_id,
        csat,
        total_requester_id_bucket,
        avg_csat,
        distinct_agent_tiers,
        min_csat,
        priority_list_item,
        MIN(min_csat) OVER (PARTITION BY min_csat, distinct_agent_tiers) AS min_csat_distinct_agent_tiers_min_min_csat,
        DENSE_RANK() OVER (PARTITION BY priority_list_item ORDER BY min_csat ASC) AS min_csat_dense_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY csat, agent_tier ORDER BY avg_csat), 3) AS avg_csat_cume_dist,
        MIN(avg_csat) OVER (PARTITION BY min_csat, distinct_agent_tiers) AS min_csat_distinct_agent_tiers_min_avg_csat,
        ROUND(min_csat - AVG(min_csat) OVER (PARTITION BY min_csat, distinct_agent_tiers), 2) AS min_csat_vs_min_csat_distinct_agent_tiers_avg,
        SIZE(COLLECT_SET(agent_tier) OVER (PARTITION BY min_csat, distinct_agent_tiers)) AS distinct_agent_tier_in_min_csat_distinct_agent_t
    FROM flattened
)
SELECT *
FROM (
    SELECT distinct_agent_tier_in_min_csat_distinct_agent_t, agent_tier, min_csat_distinct_agent_tiers_min_avg_csat
    FROM with_windows
) src
PIVOT (
    AVG(min_csat_distinct_agent_tiers_min_avg_csat)
    FOR agent_tier IN ('T1', 'T3')
)
