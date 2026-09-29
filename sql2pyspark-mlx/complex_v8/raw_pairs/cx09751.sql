WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        tags,
        CASE WHEN requester_id >= 22 THEN 'large' WHEN requester_id >= 7 THEN 'medium' ELSE 'small' END AS requester_id_tier,
        COALESCE(channel, 'UNKNOWN') AS channel_or_unknown,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE channel = 'web'
      AND HOUR(opened_at) BETWEEN 8 AND 13
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER w_agent_id_requester_id_tier_b AS seq_no,
        ROUND(requester_id_bucket - AVG(requester_id_bucket) OVER (PARTITION BY MONTH(closed_at), agent_id), 2) AS requester_id_bucket_vs_closed_at_month_agent_id_,
        RANK() OVER (PARTITION BY agent_id, requester_id_tier ORDER BY opened_at, id) AS opened_at_rank,
        MIN(requester_id_bucket) OVER (PARTITION BY MONTH(closed_at), agent_id) AS closed_at_month_agent_id_min_requester_id_bucket,
        ROUND(CUME_DIST() OVER (PARTITION BY agent_id, requester_id_tier ORDER BY opened_at), 3) AS opened_at_cume_dist,
        ROUND(AVG(requester_id_bucket) OVER (PARTITION BY MONTH(closed_at), agent_id), 2) AS closed_at_month_agent_id_avg_requester_id_bucket
    FROM base
    WINDOW w_agent_id_requester_id_tier_b AS (PARTITION BY agent_id, requester_id_tier ORDER BY opened_at, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE opened_at_rank = 1
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        opened_at,
        closed_at,
        tags,
        channel_or_unknown,
        seq_no,
        requester_id_bucket_vs_closed_at_month_agent_id_,
        opened_at_rank,
        closed_at_month_agent_id_min_requester_id_bucket,
        opened_at_cume_dist,
        closed_at_month_agent_id_avg_requester_id_bucket,
        SIZE(COLLECT_SET(requester_id_tier) OVER (PARTITION BY opened_at_rank)) AS distinct_requester_id_tier_in_opened_at_rank,
        SUM(opened_at_cume_dist) OVER (PARTITION BY opened_at_rank, channel_or_unknown ORDER BY seq_no DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_opened_at_cume_dist
    FROM first_rows
)
-- Subtotals with GROUPING SETS
SELECT
    opened_at_rank,
    channel_or_unknown,
    MAX(closed_at) AS last_closed_at
FROM with_trends
GROUP BY GROUPING SETS ((opened_at_rank, channel_or_unknown), (opened_at_rank), (channel_or_unknown))
ORDER BY opened_at_rank NULLS LAST, channel_or_unknown NULLS LAST
