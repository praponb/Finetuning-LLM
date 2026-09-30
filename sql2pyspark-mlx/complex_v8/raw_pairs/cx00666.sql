WITH valid_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS(': ', source, device) AS source_device_label
    FROM plays
    WHERE (source IS NOT NULL OR MONTH(played_at) IN (1, 3))
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        is_skipped,
        source_device_label,
        RANK() OVER (PARTITION BY source ORDER BY played_at ASC) AS played_at_rank,
        LAG(played_at, 2) OVER (PARTITION BY source ORDER BY played_at ASC, id) AS prev2_played_at,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, is_skipped), 2) AS ms_played_vs_track_id_is_skipped_avg,
        NTILE(5) OVER w_all_by_played_at AS played_at_quintile,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY track_id, is_skipped)) AS distinct_device_in_track_id_is_skipped,
        NTILE(4) OVER (PARTITION BY source ORDER BY played_at, id) AS played_at_quartile,
        SIZE(COLLECT_SET(listener_id) OVER w_track_id_is_skipped) AS distinct_listener_id_in_track_id_is_skipped,
        RANK() OVER (PARTITION BY source ORDER BY played_at ASC, id) AS played_at_rank_2
    FROM valid_plays
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id), w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(played_at_quintile / 2) * 2 AS played_at_quintile_bucket,
        FLOOR(played_at_rank_2 / 2) * 2 AS played_at_rank_2_bucket,
        LEAST(played_at_quintile, played_at_rank_2) AS least_played_at_quintile_played_at_rank_2
    FROM windowed
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY played_at_rank, played_at_rank_2_bucket ORDER BY ms_played DESC, id), 3) AS ms_played_cume_dist,
        MIN(played_at_quintile) OVER (PARTITION BY is_skipped) AS is_skipped_min_played_at_quintile,
        COUNT(*) OVER (PARTITION BY is_skipped) AS rows_in_is_skipped,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY played_at_rank ORDER BY distinct_device_in_track_id_is_skipped DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        DENSE_RANK() OVER (PARTITION BY played_at_rank, played_at_rank_2_bucket ORDER BY ms_played DESC, id) AS ms_played_dense_rank,
        LEAD(prev2_played_at, 3) OVER (PARTITION BY played_at_quintile, is_skipped ORDER BY played_at_quartile, id) AS next3_prev2_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY played_at_rank, played_at_rank_2_bucket ORDER BY ms_played DESC), 3) AS ms_played_percent_rank
    FROM derived
)
-- Final projection
SELECT
    id,
    played_at,
    ms_played,
    played_at_rank,
    prev2_played_at,
    ms_played_vs_track_id_is_skipped_avg,
    distinct_device_in_track_id_is_skipped,
    played_at_quartile,
    distinct_listener_id_in_track_id_is_skipped,
    played_at_rank_2,
    played_at_quintile_bucket,
    played_at_rank_2_bucket,
    is_skipped_min_played_at_quintile,
    rows_in_is_skipped,
    second_played_at,
    next3_prev2_played_at,
    ms_played_percent_rank
FROM with_trends
WHERE played_at_quartile BETWEEN 2 AND 7
ORDER BY played_at_rank_2 NULLS LAST, id
