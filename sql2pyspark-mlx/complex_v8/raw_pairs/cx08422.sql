WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        LOWER(REGEXP_REPLACE(TRIM(device), ' ', '_')) AS device_slug
    FROM plays
    WHERE ms_played < 220498
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY listener_id)) AS distinct_device_in_listener_id,
        ROW_NUMBER() OVER (ORDER BY ms_played ASC, id) AS ms_played_seq,
        ms_played - LAG(ms_played) OVER (PARTITION BY device ORDER BY ms_played, id) AS ms_played_change,
        LEAD(ms_played, 3) OVER (ORDER BY ms_played, id) AS next3_ms_played,
        DENSE_RANK() OVER (PARTITION BY source ORDER BY ms_played) AS ms_played_dense_rank,
        ms_played - LAG(ms_played) OVER (ORDER BY ms_played DESC, id) AS ms_played_change_2,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        SUM(ms_played) OVER w_track_id_is_skipped AS track_id_is_skipped_sum_ms_played,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_1_prior,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        MIN(ms_played) OVER (PARTITION BY track_id, is_skipped) AS track_id_is_skipped_min_ms_played
    FROM recent
    WINDOW w_track_id_is_skipped AS (PARTITION BY track_id, is_skipped)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ms_played_vs_listener_id_avg,
        ms_played_seq,
        ms_played_change,
        next3_ms_played,
        ms_played_change_2,
        first_known_ms_played,
        track_id_is_skipped_sum_ms_played,
        rows_in_listener_id,
        track_id_is_skipped_min_ms_played,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM ranked
)
SELECT
    id,
    track_id,
    ms_played,
    device,
    is_skipped,
    source,
    ms_played_vs_listener_id_avg,
    ms_played_seq,
    ms_played_change,
    next3_ms_played,
    first_known_ms_played,
    track_id_is_skipped_sum_ms_played,
    rows_in_listener_id,
    track_id_is_skipped_min_ms_played,
    device_or_unknown
FROM derived
WHERE is_skipped = TRUE
ORDER BY track_id_is_skipped_min_ms_played DESC NULLS FIRST, ms_played_seq
