WITH filtered AS (
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
        COALESCE(csat, 0) AS csat_filled,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE channel = 'chat'
      AND closed_at >= TIMESTAMP '2025-02-04 23:00:00'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        csat_filled,
        priority_clean,
        ROUND(AVG(csat_filled) OVER (PARTITION BY csat_filled, channel), 2) AS csat_filled_channel_avg_csat_filled,
        ROUND(csat_filled - AVG(csat_filled) OVER (PARTITION BY agent_id, csat), 2) AS csat_filled_vs_agent_id_csat_avg,
        SUM(csat) OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat,
        SIZE(COLLECT_SET(channel) OVER (PARTITION BY agent_id, csat)) AS distinct_channel_in_agent_id_csat,
        COUNT(*) OVER (PARTITION BY csat_filled, channel) AS rows_in_csat_filled_channel,
        ROUND(csat - AVG(csat) OVER w_agent_id_csat, 2) AS csat_vs_agent_id_csat_avg,
        DENSE_RANK() OVER (ORDER BY csat_filled DESC) AS csat_filled_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY agent_id ORDER BY csat DESC, id) AS rn,
        SUM(csat_filled) OVER (PARTITION BY agent_id ORDER BY requester_id DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_csat_filled,
        ROUND(PERCENT_RANK() OVER w_agent_id_by_csat, 3) AS csat_percent_rank
    FROM filtered
    WINDOW w_agent_id_csat AS (PARTITION BY agent_id, csat), w_agent_id_by_csat AS (PARTITION BY agent_id ORDER BY csat DESC, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(csat_filled / NULLIF(csat_percent_rank, 0), 3) AS csat_filled_per_csat_percent_rank,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM with_trends
)
-- Final projection
SELECT
    id,
    requester_id,
    agent_id,
    closed_at,
    priority,
    channel,
    csat,
    priority_clean,
    csat_filled_vs_agent_id_csat_avg,
    running_csat,
    distinct_channel_in_agent_id_csat,
    rows_in_csat_filled_channel,
    rn,
    running_csat_filled
FROM labeled
WHERE csat_filled_per_csat_percent_rank < 8.5
ORDER BY id
LIMIT 10
