WITH scoped_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 90) AS release_date_plus_90d
    FROM tracks
    WHERE release_date BETWEEN DATE '2024-04-29' AND DATE '2025-08-07'
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        release_date_plus_90d,
        ROUND(AVG(duration_sec) OVER (ORDER BY release_date_plus_90d DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_1_prior,
        LAST_VALUE(bpm) OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        SUM(bpm) OVER (PARTITION BY YEAR(release_date)) AS release_date_year_sum_bpm,
        DENSE_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC) AS bpm_dense_rank,
        LAST_VALUE(release_date_plus_90d) OVER (ORDER BY release_date_plus_90d DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_plus_90d,
        MIN(bpm) OVER w_release_date_year AS release_date_year_min_bpm,
        LAG(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm ASC, id) AS prev_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_3_prior
    FROM scoped_tracks
    WINDOW w_release_date_year AS (PARTITION BY YEAR(release_date))
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        GREATEST(duration_sec_sum_3_prior, last_bpm) AS greatest_duration_sec_sum_3_prior_last_bpm,
        bpm_dense_rank - last_bpm AS bpm_dense_rank_minus_last_bpm,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM windowed
    WHERE release_date_year_min_bpm > 669
),
windowed_2 AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(last_bpm) OVER (ORDER BY last_bpm, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS last_bpm_sum_2,
        ROUND(AVG(release_date_year_min_bpm) OVER (PARTITION BY artist_id ORDER BY bpm_dense_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS release_date_year_min_bpm_avg_7,
        ROUND(duration_sec_sum_3_prior - AVG(duration_sec_sum_3_prior) OVER w_artist_id, 2) AS duration_sec_sum_3_prior_vs_artist_id_avg,
        LAG(release_date, 3) OVER (ORDER BY last_bpm, id) AS prev3_release_date,
        ROW_NUMBER() OVER (ORDER BY last_bpm, id) AS seq_no,
        MAX(duration_sec_sum_3_prior) OVER (PARTITION BY artist_id) AS artist_id_max_duration_sec_sum_3_prior
    FROM derived
    WINDOW w_artist_id AS (PARTITION BY artist_id)
)
-- Final projection
SELECT
    id,
    release_date,
    is_explicit,
    moods,
    duration_sec_avg_1_prior,
    release_date_year_sum_bpm,
    bpm_dense_rank,
    last_release_date_plus_90d,
    release_date_year_min_bpm,
    prev_duration_sec,
    bpm_dense_rank_minus_last_bpm,
    days_since_release_date,
    last_bpm_sum_2,
    release_date_year_min_bpm_avg_7,
    duration_sec_sum_3_prior_vs_artist_id_avg,
    prev3_release_date,
    artist_id_max_duration_sec_sum_3_prior,
    QUARTER(release_date_plus_90d) AS release_date_plus_90d_quarter
FROM windowed_2
ORDER BY release_date_year_min_bpm, bpm_dense_rank_minus_last_bpm NULLS LAST
