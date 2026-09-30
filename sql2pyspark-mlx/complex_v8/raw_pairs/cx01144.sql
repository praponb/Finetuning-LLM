WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source
    FROM plays
),
combined_details_track AS (
    -- Attach track attributes
    SELECT s.*, t.bpm AS track_bpm, t.artist_id AS track_artist_id
    FROM prepared s
    JOIN tracks t ON t.id = s.track_id AND t.release_date >= DATE '2024-06-03'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER w_source_listener_id_by_track AS track_bpm_dense_rank,
        MAX(track_bpm) OVER (PARTITION BY track_artist_id) AS track_artist_id_max_track_bpm,
        LAG(track_bpm) OVER (PARTITION BY track_artist_id ORDER BY track_bpm NULLS LAST, id) AS prev_track_bpm,
        COUNT(*) OVER (PARTITION BY track_artist_id) AS rows_in_track_artist_id,
        RANK() OVER w_track_id_track_artist_id_by AS track_bpm_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY source, listener_id ORDER BY track_bpm ASC, id), 3) AS track_bpm_cume_dist,
        DENSE_RANK() OVER (PARTITION BY track_id, track_artist_id ORDER BY track_bpm ASC) AS track_bpm_dense_rank_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device, source ORDER BY track_bpm DESC, id), 3) AS track_bpm_percent_rank
    FROM combined_details_track
    WINDOW w_source_listener_id_by_track AS (PARTITION BY source, listener_id ORDER BY track_bpm ASC), w_track_id_track_artist_id_by AS (PARTITION BY track_id, track_artist_id ORDER BY track_bpm ASC)
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(track_bpm_percent_rank, 0.0) AS track_bpm_percent_rank_filled,
        CONCAT(source, '#', CAST(track_bpm AS STRING)) AS source_ref
    FROM with_windows
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        source,
        track_artist_id,
        track_bpm_dense_rank,
        track_artist_id_max_track_bpm,
        rows_in_track_artist_id,
        track_bpm_rank,
        track_bpm_cume_dist,
        track_bpm_dense_rank_2,
        track_bpm_percent_rank,
        track_bpm_percent_rank_filled,
        RANK() OVER (PARTITION BY track_id, listener_id ORDER BY track_artist_id_max_track_bpm DESC, id) AS track_artist_id_max_track_bpm_rank,
        prev_track_bpm - LAG(prev_track_bpm) OVER (PARTITION BY track_id, listener_id ORDER BY track_artist_id_max_track_bpm DESC, id) AS prev_track_bpm_change,
        LAST_VALUE(track_bpm_percent_rank) OVER (ORDER BY track_bpm_dense_rank_2 DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_track_bpm_percent_rank,
        MIN(track_bpm_rank) OVER (PARTITION BY track_bpm_rank, track_bpm_dense_rank_2) AS track_bpm_rank_track_bpm_dense_rank_2_min_track_,
        SUM(track_bpm_dense_rank_2) OVER (ORDER BY track_bpm_dense_rank_2, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS track_bpm_dense_rank_2_sum_3,
        SUM(track_bpm_cume_dist) OVER w_track_bpm_rank_track_bpm_den AS track_bpm_rank_track_bpm_dense_rank_2_sum_track_,
        ROUND(CUME_DIST() OVER (PARTITION BY track_id, listener_id ORDER BY track_artist_id_max_track_bpm DESC), 3) AS track_artist_id_max_track_bpm_cume_dist
    FROM tagged
    WINDOW w_track_bpm_rank_track_bpm_den AS (PARTITION BY track_bpm_rank, track_bpm_dense_rank_2)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    source,
    track_bpm_dense_rank,
    track_artist_id_max_track_bpm,
    track_bpm_cume_dist,
    track_bpm_dense_rank_2,
    track_bpm_percent_rank,
    track_bpm_percent_rank_filled,
    track_artist_id_max_track_bpm_rank,
    prev_track_bpm_change,
    last_track_bpm_percent_rank,
    track_bpm_dense_rank_2_sum_3,
    track_bpm_rank_track_bpm_dense_rank_2_sum_track_,
    track_artist_id_max_track_bpm_cume_dist
FROM with_windows_2
ORDER BY track_bpm_cume_dist DESC
