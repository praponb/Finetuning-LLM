WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM tracks
    WHERE (duration_sec < 312 OR release_date >= DATE '2024-08-26')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_is_explicit_artist_id_by_dur AS duration_sec_dense_rank,
        MAX(bpm) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS bpm_max_2,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_7,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_release_date_ti, 3) AS release_date_percent_rank,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_2,
        ROUND(AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_is_explicit, 0), 4) AS bpm_share_of_is_explicit,
        LAG(release_date) OVER (PARTITION BY is_explicit ORDER BY duration_sec DESC, id) AS prev_release_date
    FROM recent
    WINDOW w_is_explicit_artist_id_by_dur AS (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC NULLS LAST), w_artist_id_by_release_date_ti AS (PARTITION BY artist_id ORDER BY release_date), w_is_explicit AS (PARTITION BY is_explicit)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ABS(bpm - 96) AS bpm_distance,
        FLOOR(duration_sec_avg_7 / 50) * 50 AS duration_sec_avg_7_bucket
    FROM ranked
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec_avg_7_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_so_far,
        ROUND(duration_sec_avg_7_bucket / NULLIF(SUM(duration_sec_avg_7_bucket) OVER w_duration_sec_dense_rank_is_e, 0), 4) AS duration_sec_avg_7_bucket_share_of_duration_sec_,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id, duration_sec_dense_rank), 2) AS duration_sec_vs_artist_id_duration_sec_dense_ran,
        bpm_share_of_is_explicit - LAG(bpm_share_of_is_explicit) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec_avg_7_bucket, id) AS bpm_share_of_is_explicit_change,
        RANK() OVER (ORDER BY duration_sec, id) AS duration_sec_rank
    FROM scored
    WINDOW w_duration_sec_dense_rank_is_e AS (PARTITION BY duration_sec_dense_rank, is_explicit)
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    duration_sec_dense_rank,
    bpm_max_2,
    release_date_percent_rank,
    is_explicit_avg_bpm,
    bpm_share_of_is_explicit,
    prev_release_date,
    bpm_distance,
    duration_sec_avg_7_bucket_share_of_duration_sec_,
    duration_sec_vs_artist_id_duration_sec_dense_ran,
    bpm_share_of_is_explicit_change,
    duration_sec_rank,
    FLOOR(min_duration_sec_so_far / 1000) * 1000 AS min_duration_sec_so_far_bucket,
    DATE_TRUNC('WEEK', release_date) AS release_date_week
FROM with_windows
