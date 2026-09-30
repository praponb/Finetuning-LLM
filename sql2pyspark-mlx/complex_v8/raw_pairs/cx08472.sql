WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_TRUNC('DAY', played_at) AS played_at_day,
        CASE WHEN device IN ('speaker', 'tv') THEN 'group_a' WHEN device = 'desktop' THEN 'group_b' ELSE 'other' END AS device_group,
        CASE WHEN ms_played >= 308104 THEN 'A' WHEN ms_played >= 129336 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE NOT is_skipped
),
deduped AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        played_at_day,
        device_group,
        ms_played_tier
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id DESC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_ms_played_tier_played_at_day AS ms_played_rank,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY source), 2) AS ms_played_vs_source_avg
    FROM deduped
    WINDOW w_ms_played_tier_played_at_day AS (PARTITION BY ms_played_tier, DATE_TRUNC('MONTH', played_at_day) ORDER BY ms_played)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN ms_played_rank >= 8 THEN 'A' WHEN ms_played_rank >= 4 THEN 'B' ELSE 'C' END AS ms_played_rank_tier
    FROM metrics
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        ROUND(ms_played_vs_source_avg * 1.07, 2) AS ms_played_vs_source_avg_adj,
        LENGTH(TRIM(ms_played_tier)) AS ms_played_tier_len,
        GREATEST(ms_played_rank, ms_played_vs_source_avg) AS greatest_ms_played_rank_ms_played_vs_source_avg
    FROM derived
    WHERE listener_id IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(ms_played_vs_source_avg) OVER (PARTITION BY ms_played_rank, track_id ORDER BY greatest_ms_played_rank_ms_played_vs_source_avg DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_vs_source_avg_so_far,
        ROUND(AVG(ms_played_vs_source_avg) OVER w_device_group, 2) AS device_group_avg_ms_played_vs_source_avg,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device_group), 0), 4) AS ms_played_share_of_device_group,
        RANK() OVER (PARTITION BY ms_played_rank, track_id ORDER BY greatest_ms_played_rank_ms_played_vs_source_avg DESC NULLS FIRST) AS greatest_ms_played_rank_ms_played_vs_source_avg_,
        ROUND(CUME_DIST() OVER (ORDER BY ms_played_tier_len DESC, id), 3) AS ms_played_tier_len_cume_dist
    FROM tagged
    WINDOW w_device_group AS (PARTITION BY device_group)
)
SELECT
    is_skipped,
    greatest_ms_played_rank_ms_played_vs_source_avg_,
    ms_played_rank,
    COUNT(DISTINCT ms_played_tier) AS distinct_ms_played_tiers
FROM with_trends
GROUP BY GROUPING SETS ((is_skipped, greatest_ms_played_rank_ms_played_vs_source_avg_, ms_played_rank), (is_skipped), (greatest_ms_played_rank_ms_played_vs_source_avg_), ())
ORDER BY is_skipped NULLS LAST, greatest_ms_played_rank_ms_played_vs_source_avg_ NULLS LAST, ms_played_rank NULLS LAST
