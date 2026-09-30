WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE is_skipped = FALSE
),
combined_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        t.artist_id AS track_artist_id,
        t.bpm AS track_bpm,
        t.is_explicit AS track_is_explicit
    FROM recent s
    JOIN tracks t ON t.id = s.track_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY track_is_explicit, device ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        SUM(track_bpm) OVER (PARTITION BY device, listener_id ORDER BY played_at DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS track_bpm_sum_3_prior,
        NTILE(3) OVER (PARTITION BY track_is_explicit ORDER BY track_bpm DESC, id) AS track_bpm_tercile,
        ROUND(track_bpm - AVG(track_bpm) OVER (PARTITION BY listener_id, track_id), 2) AS track_bpm_vs_listener_id_track_id_avg,
        MIN(track_bpm) OVER (ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_track_bpm_so_far,
        LAG(track_bpm, 1, 0) OVER w_all_by_track_bpm AS prev_track_bpm,
        ROUND(track_bpm - AVG(track_bpm) OVER (PARTITION BY track_is_explicit), 2) AS track_bpm_vs_track_is_explicit_avg,
        LAG(track_bpm, 1, 0) OVER (PARTITION BY track_is_explicit ORDER BY track_bpm DESC, id) AS prev_track_bpm_2
    FROM combined_details_track
    WINDOW w_all_by_track_bpm AS (ORDER BY track_bpm DESC, id)
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        track_id,
        track_artist_id,
        track_bpm,
        second_played_at,
        track_bpm_vs_listener_id_track_id_avg,
        min_track_bpm_so_far,
        track_bpm_vs_track_is_explicit_avg,
        prev_track_bpm + prev_track_bpm_2 AS prev_track_bpm_plus_prev_track_bpm_2,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(second_played_at)) / 3600, 1) AS hours_second_played_at_to_played_at
    FROM ranked
    WHERE track_bpm_sum_3_prior BETWEEN 180 AND 468
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        second_played_at,
        min_track_bpm_so_far,
        track_bpm_vs_track_is_explicit_avg,
        prev_track_bpm_plus_prev_track_bpm_2,
        hours_second_played_at_to_played_at,
        SUM(min_track_bpm_so_far) OVER w_listener_id AS listener_id_sum_min_track_bpm_so_far,
        ROW_NUMBER() OVER (PARTITION BY track_artist_id, listener_id ORDER BY prev_track_bpm_plus_prev_track_bpm_2, id) AS seq_no,
        LAST_VALUE(track_bpm) IGNORE NULLS OVER (ORDER BY hours_second_played_at_to_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_bpm,
        NTH_VALUE(track_bpm, 2) OVER (ORDER BY hours_second_played_at_to_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_track_bpm
    FROM scored
    WINDOW w_listener_id AS (PARTITION BY listener_id)
),
stats AS (
    -- Group-level totals
    SELECT
        seq_no,
        MIN(track_bpm_vs_track_is_explicit_avg) AS min_track_bpm_vs_track_is_explicit_avg,
        MAX(min_track_bpm_so_far) AS max_min_track_bpm_so_far,
        ROUND(STDDEV(min_track_bpm_so_far), 2) AS stddev_min_track_bpm_so_far,
        PERCENTILE_APPROX(listener_id_sum_min_track_bpm_so_far, 0.5) AS median_listener_id_sum_min_track_bpm_so_far,
        COUNT(*) AS n_rows
    FROM windowed
    GROUP BY seq_no
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    seq_no,
    min_track_bpm_vs_track_is_explicit_avg,
    stddev_min_track_bpm_so_far,
    n_rows,
    seq_no - n_rows AS seq_no_minus_n_rows,
    FLOOR(seq_no / 2) * 2 AS seq_no_bucket
FROM stats
ORDER BY n_rows NULLS LAST, seq_no_bucket DESC
