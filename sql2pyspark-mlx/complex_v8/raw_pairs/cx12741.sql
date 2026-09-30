WITH recent AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        ABS(csat - 4) AS csat_distance,
        SIZE(tags) AS tags_count
    FROM tickets
    WHERE MONTH(closed_at) IN (1, 3)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        requester_id,
        channel,
        tags,
        csat_distance,
        tags_count,
        ABS(csat - 3) AS csat_distance_2,
        INITCAP(TRIM(channel)) AS channel_clean,
        CASE WHEN csat_distance >= 3 THEN 'large' WHEN csat_distance >= 1 THEN 'medium' ELSE 'small' END AS csat_distance_tier
    FROM recent
    WHERE priority IN ('P3', 'P1')
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(requester_id) OVER (PARTITION BY csat_distance_tier, csat_distance) AS csat_distance_tier_csat_distance_sum_requester_i,
        SIZE(COLLECT_SET(channel) OVER w_csat_distance_tier_csat_dist) AS distinct_channel_in_csat_distance_tier_csat_dist,
        LEAD(tags_count, 2, 0) OVER (PARTITION BY csat_distance_tier, tags_count ORDER BY tags_count DESC NULLS FIRST, id) AS next2_tags_count
    FROM scored
    WINDOW w_csat_distance_tier_csat_dist AS (PARTITION BY csat_distance_tier, csat_distance)
),
stats AS (
    -- Summarise per group
    SELECT
        csat_distance_tier,
        csat_distance_2,
        channel,
        MIN(csat_distance_2) AS min_csat_distance_2,
        MAX(csat_distance) AS max_csat_distance
    FROM sequenced
    GROUP BY csat_distance_tier, csat_distance_2, channel
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        csat_distance_tier,
        csat_distance_2,
        channel,
        ROUND(max_csat_distance - AVG(max_csat_distance) OVER (PARTITION BY csat_distance_tier), 2) AS max_csat_distance_vs_csat_distance_tier_avg,
        DENSE_RANK() OVER (ORDER BY min_csat_distance_2 NULLS LAST) AS min_csat_distance_2_dense_rank
    FROM stats
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(csat_distance_2 - AVG(csat_distance_2) OVER (PARTITION BY csat_distance_tier), 2) AS csat_distance_2_vs_csat_distance_tier_avg,
        ROUND(AVG(csat_distance_2) OVER (ORDER BY max_csat_distance_vs_csat_distance_tier_avg NULLS LAST, csat_distance_tier, csat_distance_2, channel ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS csat_distance_2_avg_1_prior
    FROM ranked
),
leaders AS (
    -- Top 5 per group
    SELECT
        csat_distance_tier,
        csat_distance_2,
        channel,
        max_csat_distance_vs_csat_distance_tier_avg,
        csat_distance_2_vs_csat_distance_tier_avg,
        csat_distance_2_avg_1_prior
    FROM with_windows
    WHERE min_csat_distance_2_dense_rank <= 5
)
-- Native SQL UNPIVOT (wide to long)
SELECT csat_distance_tier, csat_distance_2, metric_name, reading
FROM (
    SELECT csat_distance_tier, csat_distance_2, max_csat_distance_vs_csat_distance_tier_avg, csat_distance_2_avg_1_prior, csat_distance_2_vs_csat_distance_tier_avg
    FROM leaders
) src
UNPIVOT INCLUDE NULLS (
    reading FOR metric_name IN (max_csat_distance_vs_csat_distance_tier_avg, csat_distance_2_avg_1_prior, csat_distance_2_vs_csat_distance_tier_avg)
) unp
ORDER BY csat_distance_tier, csat_distance_2, metric_name
