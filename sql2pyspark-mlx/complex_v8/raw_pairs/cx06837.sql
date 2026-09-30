WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label
    FROM tracks
    WHERE duration_sec > 184
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC), 3) AS release_date_cume_dist,
        NTH_VALUE(bpm, 2) OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY release_date ASC), 3) AS release_date_percent_rank,
        RANK() OVER w_is_explicit_by_release_date AS release_date_rank,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY bpm ASC, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY release_date ASC, id) AS release_date_dense_rank,
        ROUND(bpm - AVG(bpm) OVER w_is_explicit, 2) AS bpm_vs_is_explicit_avg,
        ROUND(AVG(duration_sec) OVER (ORDER BY release_date ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_avg_2_prior,
        LAG(duration_sec, 1, 0) OVER w_is_explicit_by_release_date_2 AS prev_duration_sec,
        NTH_VALUE(release_date, 2) OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        DENSE_RANK() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS release_date_dense_rank_2,
        NTH_VALUE(duration_sec, 2) OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        bpm - LAG(bpm) OVER w_artist_id_is_explicit_by_dur AS bpm_change,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER w_release_date_month_artist_id, 0), 4) AS duration_sec_share_of_release_date_month_artist_,
        DENSE_RANK() OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec NULLS LAST) AS duration_sec_dense_rank
    FROM base_tracks
    WINDOW w_is_explicit_by_release_date AS (PARTITION BY is_explicit ORDER BY release_date DESC), w_is_explicit AS (PARTITION BY is_explicit), w_is_explicit_by_release_date_2 AS (PARTITION BY is_explicit ORDER BY release_date DESC, id), w_artist_id_is_explicit_by_dur AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec NULLS LAST, id), w_release_date_month_artist_id AS (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_id)
),
top_rows AS (
    -- Top 2 per group
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        release_date_label,
        release_date_cume_dist,
        second_bpm,
        release_date_percent_rank,
        release_date_rank,
        seq_no,
        release_date_dense_rank,
        bpm_vs_is_explicit_avg,
        duration_sec_avg_2_prior,
        prev_duration_sec,
        second_release_date,
        rows_in_artist_id,
        release_date_dense_rank_2,
        second_duration_sec,
        bpm_change,
        duration_sec_share_of_release_date_month_artist_
    FROM metrics
    WHERE duration_sec_dense_rank <= 2
),
metrics_2 AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        is_explicit,
        moods,
        release_date_label,
        release_date_cume_dist,
        release_date_percent_rank,
        release_date_rank,
        release_date_dense_rank,
        duration_sec_avg_2_prior,
        prev_duration_sec,
        second_release_date,
        rows_in_artist_id,
        release_date_dense_rank_2,
        second_duration_sec,
        bpm_change,
        duration_sec_share_of_release_date_month_artist_,
        ROUND(CUME_DIST() OVER (ORDER BY second_bpm NULLS LAST, id), 3) AS second_bpm_cume_dist,
        COUNT(*) OVER (PARTITION BY is_explicit, release_date_rank) AS rows_in_is_explicit_release_date_rank,
        MIN(prev_duration_sec) OVER (PARTITION BY is_explicit, release_date_rank) AS is_explicit_release_date_rank_min_prev_duration_
    FROM top_rows
)
SELECT
    id,
    artist_id,
    is_explicit,
    moods,
    release_date_cume_dist,
    release_date_percent_rank,
    release_date_rank,
    prev_duration_sec,
    second_release_date,
    rows_in_artist_id,
    release_date_dense_rank_2,
    bpm_change,
    duration_sec_share_of_release_date_month_artist_,
    second_bpm_cume_dist,
    rows_in_is_explicit_release_date_rank,
    is_explicit_release_date_rank_min_prev_duration_
FROM metrics_2
WHERE prev_duration_sec < 353
ORDER BY release_date_dense_rank_2 NULLS LAST
