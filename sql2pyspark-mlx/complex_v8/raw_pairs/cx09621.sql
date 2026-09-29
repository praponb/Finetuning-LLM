WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(device)) AS device_len
    FROM plays
    WHERE ms_played BETWEEN 72464 AND 236512
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_source_is_skipped_by_ms_play AS ms_played_dense_rank,
        RANK() OVER (PARTITION BY device_len ORDER BY device_len ASC) AS device_len_rank,
        FIRST_VALUE(ms_played) OVER (PARTITION BY device_len ORDER BY device_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        SUM(device_len) OVER (PARTITION BY device_len ORDER BY device_len ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS device_len_sum_3_prior,
        ROUND(device_len / NULLIF(SUM(device_len) OVER (PARTITION BY source, device), 0), 4) AS device_len_share_of_source_device,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY source, device)) AS distinct_track_id_in_source_device
    FROM valid_plays
    WINDOW w_source_is_skipped_by_ms_play AS (PARTITION BY source, is_skipped ORDER BY ms_played DESC)
)
SELECT id, metric_name, reading
FROM (
    SELECT id, device_len_sum_3_prior, device_len, first_ms_played, distinct_track_id_in_source_device
    FROM with_windows
) src
UNPIVOT (
    reading FOR metric_name IN (device_len_sum_3_prior, device_len, first_ms_played, distinct_track_id_in_source_device)
) unp
