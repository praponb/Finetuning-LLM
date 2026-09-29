WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CONCAT(source, '#', CAST(id AS STRING)) AS source_ref,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label
    FROM plays
    WHERE is_skipped = TRUE
),
joined_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source_ref,
        s.is_skipped_label,
        t.bpm AS track_bpm,
        t.is_explicit AS track_is_explicit
    FROM filtered s
    LEFT JOIN tracks t ON t.id = s.track_id AND t.bpm >= 85
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(track_bpm) OVER (ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_bpm,
        ROUND(AVG(ms_played) OVER w_listener_id, 2) AS listener_id_avg_ms_played,
        NTILE(3) OVER (ORDER BY track_bpm, id) AS track_bpm_tercile,
        SUM(track_bpm) OVER (PARTITION BY listener_id) AS listener_id_sum_track_bpm,
        FIRST_VALUE(played_at) OVER (ORDER BY track_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_played_at,
        NTILE(3) OVER (PARTITION BY is_skipped_label ORDER BY track_bpm, id) AS track_bpm_tercile_2,
        DENSE_RANK() OVER w_all_by_played_at_ties AS played_at_dense_rank,
        SUM(ms_played) OVER w_listener_id AS listener_id_sum_ms_played,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER w_listener_id, 0), 4) AS track_bpm_share_of_listener_id,
        ROUND(CUME_DIST() OVER w_is_skipped_label_by_track_bp_2, 3) AS track_bpm_cume_dist,
        ROUND(AVG(track_bpm) OVER (PARTITION BY listener_id), 2) AS listener_id_avg_track_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY track_bpm, id), 3) AS track_bpm_cume_dist_2
    FROM joined_track
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_all_by_played_at_ties AS (ORDER BY played_at DESC), w_is_skipped_label_by_track_bp_2 AS (PARTITION BY is_skipped_label ORDER BY track_bpm)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    source_ref,
    is_skipped_label,
    track_is_explicit,
    last_track_bpm,
    listener_id_avg_ms_played,
    track_bpm_tercile,
    listener_id_sum_track_bpm,
    first_played_at,
    track_bpm_tercile_2,
    listener_id_sum_ms_played,
    track_bpm_cume_dist,
    listener_id_avg_track_bpm,
    track_bpm_cume_dist_2,
    TO_DATE(first_played_at) AS first_played_at_date
FROM sequenced
ORDER BY track_bpm_tercile_2 DESC
