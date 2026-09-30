WITH prepared_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        MONTH(release_date) AS release_date_month,
        CONCAT(title, '#', CAST(id AS STRING)) AS title_ref
    FROM tracks
    WHERE duration_sec >= 184
      AND bpm BETWEEN 75 AND 170
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        LAG(duration_sec, 1, 0) OVER (ORDER BY duration_sec ASC, id) AS prev_duration_sec,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        NTILE(4) OVER (PARTITION BY artist_id, release_date_month ORDER BY duration_sec, id) AS duration_sec_quartile,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec ASC), 3) AS duration_sec_percent_rank,
        LAST_VALUE(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date_month, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        ROUND(AVG(release_date_month) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS release_date_month_avg_2_prior,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY artist_id, release_date_month ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROUND(release_date_month / NULLIF(SUM(release_date_month) OVER (PARTITION BY artist_id, release_date_month), 0), 4) AS release_date_month_share_of_artist_id_release_da,
        ROW_NUMBER() OVER (PARTITION BY artist_id, release_date_month ORDER BY duration_sec, id) AS seq_no,
        ROUND(PERCENT_RANK() OVER w_artist_id_is_explicit_by_rel_2, 3) AS release_date_month_percent_rank,
        release_date_month - LAG(release_date_month) OVER w_artist_id_is_explicit_by_rel_2 AS release_date_month_change,
        NTILE(3) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date_month, id) AS release_date_month_tercile,
        ROUND(AVG(duration_sec) OVER w_is_explicit, 2) AS is_explicit_avg_duration_sec,
        SUM(duration_sec) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_sum_6_prior
    FROM prepared_tracks
    WINDOW w_artist_id_is_explicit_by_rel_2 AS (PARTITION BY artist_id, is_explicit ORDER BY release_date_month, id), w_is_explicit AS (PARTITION BY is_explicit)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM with_trends
    WHERE seq_no <= 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER w_release_date_month_tercile_d) AS distinct_artist_id_in_release_date_month_tercile,
        LEAD(bpm) OVER (PARTITION BY distinct_artist_id_in_is_explicit ORDER BY duration_sec_sum_6_prior, id) AS next_bpm,
        DENSE_RANK() OVER w_distinct_artist_id_in_is_exp_2 AS duration_sec_sum_6_prior_dense_rank,
        first_duration_sec - LAG(first_duration_sec) OVER (PARTITION BY distinct_artist_id_in_is_explicit ORDER BY duration_sec_sum_6_prior, id) AS first_duration_sec_change,
        NTILE(3) OVER w_all_by_seq_no AS seq_no_tercile,
        COUNT(*) OVER (PARTITION BY release_date_month_tercile, duration_sec_quartile) AS rows_in_release_date_month_tercile_duration_sec_,
        LEAD(duration_sec) OVER (PARTITION BY distinct_artist_id_in_is_explicit ORDER BY release_date_month_share_of_artist_id_release_da, id) AS next_duration_sec
    FROM first_rows
    WINDOW w_release_date_month_tercile_d AS (PARTITION BY release_date_month_tercile, duration_sec_quartile), w_distinct_artist_id_in_is_exp_2 AS (PARTITION BY distinct_artist_id_in_is_explicit ORDER BY duration_sec_sum_6_prior), w_all_by_seq_no AS (ORDER BY seq_no ASC, id)
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    bpm,
    release_date_month,
    distinct_artist_id_in_is_explicit,
    prev_duration_sec,
    duration_sec_quartile,
    duration_sec_percent_rank,
    last_bpm,
    release_date_month_avg_2_prior,
    first_duration_sec,
    release_date_month_share_of_artist_id_release_da,
    seq_no,
    release_date_month_percent_rank,
    is_explicit_avg_duration_sec,
    duration_sec_sum_6_prior,
    distinct_artist_id_in_release_date_month_tercile,
    duration_sec_sum_6_prior_dense_rank,
    first_duration_sec_change,
    seq_no_tercile,
    rows_in_release_date_month_tercile_duration_sec_,
    next_duration_sec,
    INITCAP(TRIM(title)) AS title_clean,
    UPPER(TRIM(title_ref)) AS title_ref_clean
FROM windowed
WHERE first_known_bpm > 89
ORDER BY release_date_month_avg_2_prior DESC
