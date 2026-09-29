WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        DATE_TRUNC('WEEK', release_date) AS release_date_week,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        release_date,
        moods,
        release_date_week,
        title_len,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY title_len, artist_id), 0), 4) AS duration_sec_share_of_title_len_artist_id,
        LAG(duration_sec) OVER w_all_by_duration_sec AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY TO_DATE(release_date_week), artist_id ORDER BY release_date_week DESC, id) AS rn,
        DENSE_RANK() OVER (PARTITION BY title_len ORDER BY release_date_week) AS release_date_week_dense_rank,
        SUM(bpm) OVER (PARTITION BY MONTH(release_date)) AS release_date_month_sum_bpm,
        DENSE_RANK() OVER w_release_date_year_by_duratio AS duration_sec_dense_rank,
        LAG(release_date_week) OVER (PARTITION BY YEAR(release_date) ORDER BY duration_sec DESC, id) AS prev_release_date_week,
        MAX(duration_sec) OVER (PARTITION BY TO_DATE(release_date_week), artist_id ORDER BY release_date_week DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS duration_sec_max_1_prior,
        COUNT(*) OVER (PARTITION BY MONTH(release_date)) AS rows_in_release_date_month,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY title_len, artist_id), 2) AS bpm_vs_title_len_artist_id_avg,
        RANK() OVER w_release_date_month_artist_id AS release_date_week_rank,
        ROUND(AVG(title_len) OVER w_title_len_artist_id, 2) AS title_len_artist_id_avg_title_len,
        MIN(bpm) OVER w_title_len_artist_id AS title_len_artist_id_min_bpm,
        LAG(release_date, 3) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date_week DESC, id) AS prev3_release_date
    FROM filtered
    WINDOW w_all_by_duration_sec AS (ORDER BY duration_sec DESC, id), w_release_date_year_by_duratio AS (PARTITION BY YEAR(release_date) ORDER BY duration_sec DESC), w_title_len_artist_id AS (PARTITION BY title_len, artist_id), w_release_date_month_artist_id AS (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id ORDER BY release_date_week DESC)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM sequenced
    WHERE release_date_week_rank <= 5
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        moods,
        title_len,
        prev_duration_sec,
        rn,
        prev_release_date_week,
        duration_sec_max_1_prior,
        rows_in_release_date_month,
        bpm_vs_title_len_artist_id_avg,
        release_date_week_rank,
        title_len_artist_id_avg_title_len,
        title_len_artist_id_min_bpm,
        MAX(rn) OVER (PARTITION BY duration_sec_dense_rank, rn) AS duration_sec_dense_rank_rn_max_rn,
        FIRST_VALUE(duration_sec_max_1_prior) IGNORE NULLS OVER (PARTITION BY title_len ORDER BY release_date_week NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_max_1_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY rn, title_len ORDER BY bpm_vs_title_len_artist_id_avg DESC), 3) AS bpm_vs_title_len_artist_id_avg_percent_rank,
        LAG(release_date_week_dense_rank) OVER (PARTITION BY MONTH(release_date_week) ORDER BY prev3_release_date, id) AS prev_release_date_week_dense_rank,
        COUNT(*) OVER w_duration_sec_dense_rank_rn AS rows_in_duration_sec_dense_rank_rn,
        RANK() OVER (PARTITION BY rn, title_len ORDER BY bpm_vs_title_len_artist_id_avg DESC) AS bpm_vs_title_len_artist_id_avg_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY title_len ORDER BY release_date_week NULLS LAST, id), 3) AS release_date_week_cume_dist
    FROM top_rows
    WINDOW w_duration_sec_dense_rank_rn AS (PARTITION BY duration_sec_dense_rank, rn)
)
SELECT
    id,
    moods,
    rn,
    prev_release_date_week,
    duration_sec_max_1_prior,
    bpm_vs_title_len_artist_id_avg,
    release_date_week_rank,
    title_len_artist_id_min_bpm,
    duration_sec_dense_rank_rn_max_rn,
    first_known_duration_sec_max_1_prior,
    prev_release_date_week_dense_rank,
    rows_in_duration_sec_dense_rank_rn,
    bpm_vs_title_len_artist_id_avg_rank,
    release_date_week_cume_dist,
    duration_sec_max_1_prior + release_date_week_rank AS duration_sec_max_1_prior_plus_release_date_week_,
    HOUR(prev_release_date_week) AS prev_release_date_week_hour
FROM with_trends
