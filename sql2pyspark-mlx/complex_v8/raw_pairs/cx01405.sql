WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE artist_id >= 3
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)), 0), 4) AS bpm_share_of_release_date_month,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS bpm_vs_artist_id_is_explicit_avg,
        SUM(bpm) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        LAG(release_date) OVER w_release_date_year_artist_id AS prev_release_date,
        DENSE_RANK() OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec DESC) AS duration_sec_dense_rank,
        MAX(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)) AS release_date_month_max_duration_sec,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date)) AS rows_in_release_date_month,
        SUM(duration_sec) OVER w_release_date_month AS release_date_month_sum_duration_sec,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY MONTH(release_date) ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_1_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec DESC), 3) AS duration_sec_cume_dist,
        ROUND(AVG(bpm) OVER (PARTITION BY MONTH(release_date) ORDER BY bpm ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_2,
        LEAD(duration_sec) OVER w_release_date_month_by_bpm AS next_duration_sec
    FROM base_tracks
    WINDOW w_release_date_year_artist_id AS (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec DESC, id), w_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', release_date)), w_release_date_month_by_bpm AS (PARTITION BY MONTH(release_date) ORDER BY bpm ASC NULLS LAST, id)
),
top_per_group AS (
    -- Top 5 per group
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        bpm_share_of_release_date_month,
        rows_in_artist_id_is_explicit,
        bpm_vs_artist_id_is_explicit_avg,
        running_bpm,
        prev_release_date,
        release_date_month_max_duration_sec,
        rows_in_release_date_month,
        release_date_month_sum_duration_sec,
        first_known_bpm,
        bpm_avg_1_prior,
        duration_sec_cume_dist,
        bpm_avg_2,
        next_duration_sec
    FROM sequenced
    WHERE duration_sec_dense_rank <= 5
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    moods,
    bpm_share_of_release_date_month,
    rows_in_artist_id_is_explicit,
    bpm_vs_artist_id_is_explicit_avg,
    prev_release_date,
    release_date_month_max_duration_sec,
    release_date_month_sum_duration_sec,
    first_known_bpm,
    bpm_avg_1_prior,
    duration_sec_cume_dist,
    bpm_avg_2,
    next_duration_sec,
    COALESCE(next_duration_sec, 0) AS next_duration_sec_filled
FROM top_per_group
ORDER BY next_duration_sec DESC, artist_id DESC
