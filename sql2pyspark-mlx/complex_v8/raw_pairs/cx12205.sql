WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        ADD_MONTHS(release_date, 3) AS release_date_plus_3m
    FROM tracks
    WHERE artist_id >= 9
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        release_date_plus_3m,
        MIN(bpm) OVER (PARTITION BY artist_id, MONTH(release_date)) AS artist_id_release_date_month_min_bpm,
        LAG(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date NULLS LAST, id) AS prev_duration_sec,
        RANK() OVER (PARTITION BY artist_id ORDER BY bpm DESC, id) AS bpm_rank,
        SUM(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date NULLS LAST, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_sum_4,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date NULLS LAST, id) AS seq_no,
        MIN(bpm) OVER (PARTITION BY MONTH(release_date), is_explicit) AS release_date_month_is_explicit_min_bpm,
        ROUND(AVG(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS duration_sec_avg_2,
        LAST_VALUE(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_duration_sec,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec), 3) AS duration_sec_percent_rank
    FROM scoped_tracks
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        duration_sec,
        release_date_plus_3m,
        seq_no,
        release_date_month_is_explicit_min_bpm,
        last_duration_sec,
        duration_sec_percent_rank,
        LAST_VALUE(release_date) OVER (PARTITION BY seq_no, bpm_rank ORDER BY duration_sec_sum_4 NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        COUNT(*) OVER (PARTITION BY bpm_rank, YEAR(release_date_plus_3m)) AS rows_in_bpm_rank_release_date_plus_3m_year,
        NTH_VALUE(last_duration_sec, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date_plus_3m) ORDER BY artist_id_release_date_month_min_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_duration_sec,
        LAG(duration_sec_percent_rank, 3) OVER (PARTITION BY bpm_rank ORDER BY seq_no, id) AS prev3_duration_sec_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no, bpm_rank ORDER BY duration_sec_sum_4 NULLS LAST), 3) AS duration_sec_sum_4_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY seq_no, bpm_rank ORDER BY duration_sec_sum_4 NULLS LAST), 3) AS duration_sec_sum_4_percent_rank
    FROM ranked
)
SELECT
    id,
    duration_sec,
    release_date_plus_3m,
    seq_no,
    last_duration_sec,
    rows_in_bpm_rank_release_date_plus_3m_year,
    second_last_duration_sec,
    prev3_duration_sec_percent_rank,
    duration_sec_sum_4_cume_dist,
    duration_sec_sum_4_percent_rank,
    ROUND(last_duration_sec / NULLIF(duration_sec, 0), 3) AS last_duration_sec_per_duration_sec,
    CONCAT(title, '#', CAST(duration_sec AS STRING)) AS title_ref
FROM metrics
ORDER BY release_date_plus_3m DESC, second_last_duration_sec NULLS LAST
