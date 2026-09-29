WITH filtered AS (
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
    WHERE listener_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        ROW_NUMBER() OVER w_device_listener_id_by_played AS seq_no,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        LAST_VALUE(played_at) OVER (PARTITION BY source, track_id ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY TO_DATE(played_at), listener_id), 2) AS played_at_to_date_listener_id_avg_ms_played,
        ms_played - LAG(ms_played) OVER (PARTITION BY source, track_id ORDER BY ms_played, id) AS ms_played_change,
        MAX(ms_played) OVER (PARTITION BY listener_id ORDER BY played_at DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_max_2,
        ROUND(AVG(ms_played) OVER w_device_track_id, 2) AS device_track_id_avg_ms_played,
        ROW_NUMBER() OVER (ORDER BY ms_played, id) AS rn
    FROM filtered
    WINDOW w_device_listener_id_by_played AS (PARTITION BY device, listener_id ORDER BY played_at DESC, id), w_device_track_id AS (PARTITION BY device, track_id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        played_at_cume_dist,
        last_played_at,
        played_at_to_date_listener_id_avg_ms_played,
        ms_played_change,
        ms_played_max_2,
        device_track_id_avg_ms_played,
        rn
    FROM windowed
    WHERE seq_no = 1
      AND played_at_to_date_listener_id_avg_ms_played >= 44500.0
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
        played_at_cume_dist,
        last_played_at,
        ms_played_change,
        device_track_id_avg_ms_played,
        rn,
        FIRST_VALUE(last_played_at) IGNORE NULLS OVER (PARTITION BY rn, source ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_last_played_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped, listener_id ORDER BY last_played_at ASC), 3) AS last_played_at_percent_rank,
        COUNT(*) OVER (PARTITION BY is_skipped, listener_id ORDER BY last_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(device) OVER w_is_skipped) AS distinct_device_in_is_skipped,
        NTILE(10) OVER w_rn_source_by_ms_played AS ms_played_decile
    FROM first_rows
    WINDOW w_is_skipped AS (PARTITION BY is_skipped), w_rn_source_by_ms_played AS (PARTITION BY rn, source ORDER BY ms_played, id)
)
SELECT
    id,
    track_id,
    played_at,
    device,
    last_played_at,
    ms_played_change,
    rn,
    distinct_device_in_is_skipped,
    ms_played_decile,
    SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
    running_count - ms_played_change AS running_count_minus_ms_played_change
FROM ranked
ORDER BY track_id DESC, device DESC NULLS FIRST
