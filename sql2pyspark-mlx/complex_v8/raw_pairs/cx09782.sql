WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
    WHERE (bpm <= 165 OR NOT is_explicit)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY release_date_quarter ORDER BY bpm, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_3_prior,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY release_date_quarter, artist_id ORDER BY release_date_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        SUM(release_date_quarter) OVER w_artist_id AS artist_id_sum_release_date_quarter,
        DENSE_RANK() OVER (ORDER BY release_date_quarter NULLS LAST, id) AS release_date_quarter_dense_rank,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec_2,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date_quarter NULLS LAST), 3) AS release_date_quarter_percent_rank,
        NTH_VALUE(release_date_quarter, 2) OVER (PARTITION BY release_date_quarter ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_quarter,
        LAG(bpm) OVER w_all_by_release_date_quarter AS prev_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY release_date_quarter ORDER BY bpm, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7,
        FIRST_VALUE(release_date_quarter) OVER (PARTITION BY release_date_quarter, artist_id ORDER BY release_date_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date_quarter,
        NTH_VALUE(release_date_quarter, 2) OVER (PARTITION BY release_date_quarter, artist_id ORDER BY release_date_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_quarter_2
    FROM recent
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_all_by_release_date_quarter AS (ORDER BY release_date_quarter NULLS LAST, id)
),
leaders AS (
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
        release_date_quarter,
        duration_sec_sum_3_prior,
        second_duration_sec,
        artist_id_sum_release_date_quarter,
        second_duration_sec_2,
        rows_in_artist_id,
        release_date_quarter_percent_rank,
        second_release_date_quarter,
        prev_bpm,
        bpm_avg_7,
        first_release_date_quarter,
        second_release_date_quarter_2
    FROM metrics
    WHERE release_date_quarter_dense_rank = 1
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods,
        release_date_quarter,
        second_duration_sec,
        artist_id_sum_release_date_quarter,
        rows_in_artist_id,
        second_release_date_quarter,
        bpm_avg_7,
        second_release_date_quarter_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY second_release_date_quarter_2 ORDER BY prev_bpm DESC NULLS FIRST), 3) AS prev_bpm_percent_rank,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        ROUND(second_duration_sec - AVG(second_duration_sec) OVER (PARTITION BY is_explicit), 2) AS second_duration_sec_vs_is_explicit_avg,
        DENSE_RANK() OVER (PARTITION BY release_date_quarter, is_explicit ORDER BY prev_bpm DESC) AS prev_bpm_dense_rank,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit
    FROM leaders
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    moods,
    second_duration_sec,
    artist_id_sum_release_date_quarter,
    rows_in_artist_id,
    second_release_date_quarter,
    bpm_avg_7,
    second_release_date_quarter_2,
    prev_bpm_percent_rank,
    distinct_artist_id_in_is_explicit,
    prev_bpm_dense_rank,
    LOWER(TRIM(title)) AS title_clean
FROM with_trends
ORDER BY distinct_artist_id_in_is_explicit DESC
