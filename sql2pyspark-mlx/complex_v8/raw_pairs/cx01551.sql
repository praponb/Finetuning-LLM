WITH base_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CASE WHEN ms_played >= 301865 THEN 'large' WHEN ms_played >= 112786 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE is_skipped = TRUE
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        ms_played_tier,
        DENSE_RANK() OVER w_all_by_played_at_ties AS played_at_dense_rank,
        MAX(ms_played) OVER w_device AS device_max_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER w_is_skipped, 2) AS ms_played_vs_is_skipped_avg,
        ROW_NUMBER() OVER w_all_by_played_at AS seq_no,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device), 0), 4) AS ms_played_share_of_device,
        ROW_NUMBER() OVER (ORDER BY ms_played ASC, id) AS ms_played_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY ms_played_tier ORDER BY ms_played DESC), 3) AS ms_played_cume_dist,
        SUM(ms_played) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        LEAD(ms_played, 2, 0) OVER (PARTITION BY ms_played_tier, device ORDER BY ms_played, id) AS next2_ms_played,
        ROUND(AVG(ms_played) OVER (PARTITION BY device), 2) AS device_avg_ms_played,
        ROW_NUMBER() OVER (PARTITION BY track_id ORDER BY ms_played, id) AS rn,
        ROUND(ms_played - AVG(ms_played) OVER w_device, 2) AS ms_played_vs_device_avg
    FROM base_plays
    WINDOW w_all_by_played_at_ties AS (ORDER BY played_at DESC), w_device AS (PARTITION BY device), w_is_skipped AS (PARTITION BY is_skipped), w_all_by_played_at AS (ORDER BY played_at DESC, id)
)
SELECT
    id,
    ms_played,
    device,
    is_skipped,
    ms_played_tier,
    played_at_dense_rank,
    seq_no,
    second_played_at,
    ms_played_share_of_device,
    ms_played_seq,
    running_ms_played,
    next2_ms_played,
    device_avg_ms_played,
    rn,
    ms_played_vs_device_avg
FROM with_trends
WHERE ms_played_vs_is_skipped_avg > -240000.0
ORDER BY ms_played DESC
