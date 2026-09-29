WITH base_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE (device = 'desktop' OR ms_played >= 52009)
),
enriched_track AS (
    -- Attach track attributes
    SELECT s.*, t.bpm AS track_bpm, t.artist_id AS track_artist_id
    FROM base_plays s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.bpm < 166
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        source,
        is_skipped_label,
        is_skipped_flag,
        ROUND(track_bpm - AVG(track_bpm) OVER w_device, 2) AS track_bpm_vs_device_avg,
        track_bpm - LAG(track_bpm) OVER w_all_by_track_bpm AS track_bpm_change,
        MIN(is_skipped_flag) OVER (ORDER BY track_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_is_skipped_flag_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, source ORDER BY ms_played DESC, id), 3) AS ms_played_cume_dist,
        FIRST_VALUE(ms_played) OVER (ORDER BY track_bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        NTH_VALUE(track_bpm, 2) OVER (ORDER BY track_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_bpm,
        ROUND(is_skipped_flag - AVG(is_skipped_flag) OVER w_track_artist_id_is_skipped_f, 2) AS is_skipped_flag_vs_track_artist_id_is_skipped_fl,
        SUM(is_skipped_flag) OVER w_track_id_is_skipped_label AS track_id_is_skipped_label_sum_is_skipped_flag
    FROM enriched_track
    WINDOW w_device AS (PARTITION BY device), w_all_by_track_bpm AS (ORDER BY track_bpm DESC, id), w_track_artist_id_is_skipped_f AS (PARTITION BY track_artist_id, is_skipped_flag), w_track_id_is_skipped_label AS (PARTITION BY track_id, is_skipped_label)
)
SELECT
    id,
    track_id,
    ms_played,
    source,
    is_skipped_label,
    is_skipped_flag,
    ms_played_cume_dist,
    first_ms_played,
    second_track_bpm
FROM with_windows
ORDER BY is_skipped_flag DESC
