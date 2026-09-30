WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE ms_played BETWEEN 99376 AND 239955
      AND track_id >= 23
),
combined_details_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.is_skipped,
        s.source,
        t.release_date AS track_release_date,
        t.duration_sec AS track_duration_sec
    FROM base s
    LEFT JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY source, listener_id), 2) AS track_duration_sec_vs_source_listener_id_avg,
        RANK() OVER (PARTITION BY source, track_id ORDER BY track_duration_sec) AS track_duration_sec_rank,
        DENSE_RANK() OVER w_track_id_is_skipped_by_track AS track_release_date_dense_rank,
        MIN(track_duration_sec) OVER w_source_listener_id AS source_listener_id_min_track_duration_sec,
        DENSE_RANK() OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC) AS played_at_dense_rank,
        SUM(track_duration_sec) OVER (PARTITION BY track_id, is_skipped ORDER BY track_release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_track_duration_sec,
        track_duration_sec - LAG(track_duration_sec) OVER (PARTITION BY source, track_id ORDER BY track_duration_sec, id) AS track_duration_sec_change,
        DENSE_RANK() OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id) AS played_at_dense_rank_3
    FROM combined_details_track
    WINDOW w_track_id_is_skipped_by_track AS (PARTITION BY track_id, is_skipped ORDER BY track_release_date DESC), w_source_listener_id AS (PARTITION BY source, listener_id)
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        track_release_date,
        track_duration_sec_vs_source_listener_id_avg,
        track_duration_sec_rank,
        track_release_date_dense_rank,
        source_listener_id_min_track_duration_sec,
        track_duration_sec_change,
        played_at_dense_rank_3,
        YEAR(track_release_date) AS track_release_date_year,
        LAST_DAY(track_release_date) AS track_release_date_month_end
    FROM ranked
    WHERE track_release_date_dense_rank BETWEEN 3 AND 9
)
-- Final projection
SELECT
    id,
    track_id,
    played_at,
    is_skipped,
    track_release_date,
    track_duration_sec_vs_source_listener_id_avg,
    track_release_date_dense_rank,
    source_listener_id_min_track_duration_sec,
    track_duration_sec_change,
    track_release_date_year,
    track_release_date_month_end,
    IF(is_skipped, 'yes', 'no') AS is_skipped_label
FROM enriched
WHERE track_duration_sec_rank >= 3
ORDER BY is_skipped_label NULLS LAST, played_at NULLS LAST
