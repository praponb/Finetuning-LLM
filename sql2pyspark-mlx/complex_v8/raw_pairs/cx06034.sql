WITH base_plays AS (
    -- Keep valid rows and derive helper columns
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
    WHERE ms_played >= 56858
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.username AS listener_username,
        t.bpm AS track_bpm,
        t.is_explicit AS track_is_explicit
    FROM base_plays s
    LEFT JOIN listeners l ON l.id = s.listener_id AND l.signup_date >= DATE '2024-09-19'
    LEFT JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(track_bpm) OVER (PARTITION BY device, is_skipped) AS device_is_skipped_min_track_bpm,
        COUNT(*) OVER (PARTITION BY track_is_explicit ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        SIZE(COLLECT_SET(listener_id) OVER w_device_is_skipped) AS distinct_listener_id_in_device_is_skipped,
        LAST_VALUE(track_bpm) IGNORE NULLS OVER (PARTITION BY track_is_explicit ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_bpm,
        ROW_NUMBER() OVER (PARTITION BY track_is_explicit ORDER BY played_at DESC, id) AS seq_no,
        ms_played - LAG(ms_played) OVER w_listener_id_is_skipped_by_ms AS ms_played_change,
        ROUND(AVG(ms_played) OVER (PARTITION BY device, is_skipped), 2) AS device_is_skipped_avg_ms_played,
        MIN(track_bpm) OVER (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_track_bpm_so_far,
        LAG(ms_played, 2) OVER w_is_skipped_by_ms_played AS prev2_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped ORDER BY ms_played DESC NULLS FIRST), 3) AS ms_played_percent_rank,
        ms_played - LAG(ms_played) OVER (PARTITION BY is_skipped ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_change_2,
        COUNT(*) OVER w_device_is_skipped AS rows_in_device_is_skipped,
        LEAD(ms_played, 1, 0) OVER w_listener_id_is_skipped_by_ms AS next_ms_played
    FROM with_details
    WINDOW w_device_is_skipped AS (PARTITION BY device, is_skipped), w_listener_id_is_skipped_by_ms AS (PARTITION BY listener_id, is_skipped ORDER BY ms_played DESC, id), w_is_skipped_by_ms_played AS (PARTITION BY is_skipped ORDER BY ms_played DESC NULLS FIRST, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE seq_no = 1
      AND last_known_track_bpm < 142
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    ms_played,
    is_skipped,
    source,
    listener_username,
    device_is_skipped_min_track_bpm,
    distinct_listener_id_in_device_is_skipped,
    last_known_track_bpm,
    seq_no,
    min_track_bpm_so_far,
    prev2_ms_played,
    ms_played_change_2,
    rows_in_device_is_skipped,
    next_ms_played,
    SUBSTRING(TRIM(listener_username), 1, 2) AS listener_username_prefix,
    CONCAT(listener_username, '#', CAST(track_bpm AS STRING)) AS listener_username_ref
FROM first_rows
ORDER BY played_at NULLS LAST, ms_played DESC, id
LIMIT 10
