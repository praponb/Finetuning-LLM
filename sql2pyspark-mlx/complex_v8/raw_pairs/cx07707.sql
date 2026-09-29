WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id, source), 0), 4) AS ms_played_share_of_listener_id_source,
        LEAD(played_at) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id) AS next_played_at,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY device, listener_id), 0), 4) AS ms_played_share_of_device_listener_id,
        SIZE(COLLECT_SET(source) OVER w_device_listener_id) AS distinct_source_in_device_listener_id,
        LEAD(ms_played, 3) OVER (ORDER BY played_at, id) AS next3_ms_played,
        NTILE(10) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at DESC NULLS FIRST, id) AS played_at_decile,
        COUNT(*) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        SUM(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        NTILE(4) OVER (PARTITION BY is_skipped, listener_id ORDER BY ms_played DESC, id) AS ms_played_quartile,
        DENSE_RANK() OVER w_played_at_to_date_is_skipped AS played_at_dense_rank,
        MIN(ms_played) OVER (PARTITION BY track_id, is_skipped ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_ms_played_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id, is_skipped ORDER BY played_at DESC NULLS FIRST, id), 3) AS played_at_percent_rank
    FROM recent_plays
    WINDOW w_device_listener_id AS (PARTITION BY device, listener_id), w_played_at_to_date_is_skipped AS (PARTITION BY TO_DATE(played_at), is_skipped ORDER BY played_at DESC, id)
)
SELECT
    id,
    listener_id,
    track_id,
    ms_played,
    device,
    source,
    ms_played_share_of_device_listener_id,
    distinct_source_in_device_listener_id,
    next3_ms_played,
    played_at_decile,
    ms_played_avg_2_prior,
    running_ms_played,
    ms_played_quartile,
    played_at_dense_rank,
    min_ms_played_so_far,
    played_at_percent_rank,
    COALESCE(device, 'UNKNOWN') AS device_or_unknown
FROM ranked
WHERE running_ms_played > 1603000
ORDER BY running_ms_played NULLS LAST, source
