WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE is_skipped = TRUE
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        track_id,
        is_skipped,
        source,
        LAST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played,
        ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS ms_played_seq,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_source, 0), 4) AS ms_played_share_of_source,
        FIRST_VALUE(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        DENSE_RANK() OVER w_is_skipped_device_by_ms_play AS ms_played_dense_rank,
        NTILE(10) OVER (PARTITION BY is_skipped ORDER BY ms_played, id) AS ms_played_decile,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY source)) AS distinct_listener_id_in_source,
        SIZE(COLLECT_SET(listener_id) OVER w_is_skipped) AS distinct_listener_id_in_is_skipped,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ms_played - LAG(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played, id) AS ms_played_change,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY ms_played), 3) AS ms_played_cume_dist,
        SUM(ms_played) OVER (PARTITION BY is_skipped) AS is_skipped_sum_ms_played,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY is_skipped, device ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_2
    FROM scoped
    WINDOW w_source AS (PARTITION BY source), w_is_skipped_device_by_ms_play AS (PARTITION BY is_skipped, device ORDER BY ms_played DESC, id), w_is_skipped AS (PARTITION BY is_skipped)
)
SELECT
    id,
    track_id,
    source,
    last_known_ms_played,
    ms_played_seq,
    ms_played_share_of_source,
    first_ms_played,
    ms_played_dense_rank,
    ms_played_decile,
    distinct_listener_id_in_source,
    distinct_listener_id_in_is_skipped,
    second_ms_played,
    ms_played_change,
    first_known_ms_played,
    ms_played_cume_dist,
    is_skipped_sum_ms_played
FROM windowed
WHERE last_known_ms_played < 246779
ORDER BY ms_played_dense_rank DESC
