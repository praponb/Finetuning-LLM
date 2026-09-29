WITH scoped AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'workout')
      AND bpm <= 143
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        duration_sec_per_bpm,
        moods_count,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY bpm), 3) AS bpm_percent_rank,
        LAG(duration_sec) OVER (ORDER BY duration_sec_per_bpm DESC NULLS FIRST, id) AS prev_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec_per_bpm ASC NULLS LAST, id) AS seq_no,
        ROUND(duration_sec_per_bpm / NULLIF(SUM(duration_sec_per_bpm) OVER w_release_date_year, 0), 4) AS duration_sec_per_bpm_share_of_release_date_year,
        ROUND(AVG(moods_count) OVER w_release_date_year, 2) AS release_date_year_avg_moods_count,
        ROUND(AVG(moods_count) OVER (ORDER BY duration_sec_per_bpm DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS moods_count_avg_6_prior,
        LEAD(duration_sec, 2) OVER (ORDER BY duration_sec_per_bpm, id) AS next2_duration_sec,
        moods_count - LAG(moods_count) OVER (PARTITION BY moods_count, is_explicit ORDER BY release_date, id) AS moods_count_change,
        duration_sec_per_bpm - LAG(duration_sec_per_bpm) OVER (ORDER BY duration_sec_per_bpm, id) AS duration_sec_per_bpm_change,
        DENSE_RANK() OVER (ORDER BY duration_sec_per_bpm) AS duration_sec_per_bpm_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec_per_bpm), 3) AS duration_sec_per_bpm_percent_rank,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY YEAR(release_date)), 2) AS bpm_vs_release_date_year_avg,
        MIN(duration_sec) OVER (PARTITION BY YEAR(release_date)) AS release_date_year_min_duration_sec,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id ORDER BY duration_sec_per_bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROUND(AVG(moods_count) OVER w_is_explicit_artist_id, 2) AS is_explicit_artist_id_avg_moods_count
    FROM scoped
    WINDOW w_release_date_year AS (PARTITION BY YEAR(release_date)), w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        moods,
        duration_sec_per_bpm,
        moods_count,
        prev_duration_sec,
        seq_no,
        release_date_year_avg_moods_count,
        moods_count_avg_6_prior,
        next2_duration_sec,
        moods_count_change,
        duration_sec_per_bpm_change,
        duration_sec_per_bpm_dense_rank,
        duration_sec_per_bpm_percent_rank,
        bpm_vs_release_date_year_avg,
        release_date_year_min_duration_sec,
        first_known_release_date,
        is_explicit_artist_id_avg_moods_count,
        ABS(bpm - 147) AS bpm_distance
    FROM with_windows
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        LEAD(moods_count_avg_6_prior, 2, 0) OVER (PARTITION BY seq_no, moods_count_change ORDER BY release_date_year_min_duration_sec DESC, id) AS next2_moods_count_avg_6_prior,
        MIN(duration_sec) OVER (PARTITION BY moods_count_change, seq_no) AS moods_count_change_seq_no_min_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no, moods_count_change ORDER BY release_date_year_min_duration_sec DESC, id), 3) AS release_date_year_min_duration_sec_cume_dist,
        MIN(moods_count) OVER (ORDER BY duration_sec_per_bpm_percent_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_moods_count_so_far,
        LEAD(duration_sec_per_bpm_change, 1, 0) OVER (ORDER BY duration_sec_per_bpm_percent_rank, id) AS next_duration_sec_per_bpm_change,
        RANK() OVER w_is_explicit_by_duration_sec AS duration_sec_per_bpm_percent_rank_rank,
        ROW_NUMBER() OVER w_all_by_duration_sec_per_bpm_6 AS seq_no_2,
        ROUND(AVG(duration_sec_per_bpm) OVER (PARTITION BY moods_count_change, seq_no), 2) AS moods_count_change_seq_no_avg_duration_sec_per_b
    FROM annotated
    WINDOW w_is_explicit_by_duration_sec AS (PARTITION BY is_explicit ORDER BY duration_sec_per_bpm_percent_rank), w_all_by_duration_sec_per_bpm_6 AS (ORDER BY duration_sec_per_bpm_percent_rank, id)
)
-- Final projection
SELECT
    id,
    is_explicit,
    bpm,
    duration_sec_per_bpm,
    prev_duration_sec,
    seq_no,
    release_date_year_avg_moods_count,
    moods_count_avg_6_prior,
    next2_duration_sec,
    moods_count_change,
    duration_sec_per_bpm_change,
    duration_sec_per_bpm_percent_rank,
    bpm_vs_release_date_year_avg,
    release_date_year_min_duration_sec,
    bpm_distance,
    next2_moods_count_avg_6_prior,
    release_date_year_min_duration_sec_cume_dist,
    next_duration_sec_per_bpm_change,
    duration_sec_per_bpm_percent_rank_rank,
    seq_no_2,
    YEAR(first_known_release_date) AS first_known_release_date_year,
    ROUND(prev_duration_sec / NULLIF(seq_no_2, 0), 3) AS prev_duration_sec_per_seq_no_2
FROM metrics
ORDER BY bpm DESC NULLS FIRST
