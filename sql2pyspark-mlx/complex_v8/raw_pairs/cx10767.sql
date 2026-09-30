WITH cleaned AS (
    -- Scope the tracks rows
    SELECT *
    FROM tracks
    WHERE bpm <= 156
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUM(duration_sec) OVER w_artist_id_is_explicit AS artist_id_is_explicit_sum_duration_sec,
        duration_sec - LAG(duration_sec) OVER (ORDER BY release_date, id) AS duration_sec_change,
        LAG(duration_sec, 1, 0) OVER w_all_by_release_date_2 AS prev_duration_sec,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS artist_id_is_explicit_avg_bpm,
        DENSE_RANK() OVER (ORDER BY release_date, id) AS release_date_dense_rank,
        MIN(duration_sec) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_min_duration_sec,
        FIRST_VALUE(duration_sec) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER w_artist_id_is_explicit, 2) AS duration_sec_vs_artist_id_is_explicit_avg,
        bpm - LAG(bpm) OVER (ORDER BY release_date DESC, id) AS bpm_change,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit
    FROM cleaned
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_all_by_release_date_2 AS (ORDER BY release_date DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        artist_id_is_explicit_sum_duration_sec,
        duration_sec_change,
        prev_duration_sec,
        artist_id_is_explicit_avg_bpm,
        artist_id_is_explicit_min_duration_sec,
        first_duration_sec,
        duration_sec_vs_artist_id_is_explicit_avg,
        bpm_change,
        rows_in_artist_id_is_explicit
    FROM with_windows
    WHERE release_date_dense_rank = 1
      AND artist_id <= 14
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    artist_id_is_explicit_sum_duration_sec,
    duration_sec_change,
    prev_duration_sec,
    artist_id_is_explicit_avg_bpm,
    duration_sec_vs_artist_id_is_explicit_avg,
    bpm_change,
    rows_in_artist_id_is_explicit,
    TRUNC(release_date, 'MM') AS release_date_month_start,
    ARRAY_CONTAINS(moods, 'sad') AS has_sad
FROM top_per_group
ORDER BY release_date_month_start DESC, id
