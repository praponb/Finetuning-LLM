WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE is_skipped = TRUE
      AND source <> 'radio'
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped, source ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_1_prior,
        COUNT(*) OVER (PARTITION BY source, is_skipped ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LAG(ms_played) OVER (PARTITION BY source, is_skipped ORDER BY ms_played, id) AS prev_ms_played,
        COUNT(*) OVER w_is_skipped_track_id AS rows_in_is_skipped_track_id,
        ROUND(ms_played_bucket / NULLIF(SUM(ms_played_bucket) OVER (PARTITION BY listener_id, device), 0), 4) AS ms_played_bucket_share_of_listener_id_device,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY is_skipped, track_id)) AS distinct_listener_id_in_is_skipped_track_id,
        MIN(ms_played_bucket) OVER (PARTITION BY listener_id, device) AS listener_id_device_min_ms_played_bucket,
        DENSE_RANK() OVER (PARTITION BY device ORDER BY ms_played, id) AS ms_played_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_id, source ORDER BY ms_played ASC NULLS LAST), 3) AS ms_played_percent_rank,
        SIZE(COLLECT_SET(track_id) OVER w_listener_id_device) AS distinct_track_id_in_listener_id_device,
        LAST_VALUE(ms_played_bucket) IGNORE NULLS OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_bucket,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY track_id, device), 2) AS ms_played_vs_track_id_device_avg
    FROM valid
    WINDOW w_is_skipped_track_id AS (PARTITION BY is_skipped, track_id), w_listener_id_device AS (PARTITION BY listener_id, device)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM with_trends
    WHERE ms_played_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played,
    device,
    is_skipped,
    source,
    ms_played_bucket,
    running_count,
    prev_ms_played,
    distinct_listener_id_in_is_skipped_track_id,
    listener_id_device_min_ms_played_bucket,
    ms_played_dense_rank,
    ms_played_percent_rank,
    distinct_track_id_in_listener_id_device,
    first_ms_played,
    ms_played_vs_track_id_device_avg,
    LENGTH(TRIM(source)) AS source_len
FROM top_rows
ORDER BY first_ms_played
