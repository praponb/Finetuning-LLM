WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
    WHERE is_explicit = TRUE
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAG(duration_sec_per_bpm) OVER (ORDER BY bpm, id) AS prev_duration_sec_per_bpm,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        LEAD(release_date) OVER w_artist_id_by_duration_sec_pe AS next_release_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm DESC, id), 3) AS bpm_percent_rank,
        ROW_NUMBER() OVER (ORDER BY bpm DESC, id) AS rn,
        ROUND(duration_sec_per_bpm / NULLIF(SUM(duration_sec_per_bpm) OVER (PARTITION BY artist_id), 0), 4) AS duration_sec_per_bpm_share_of_artist_id,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        COUNT(*) OVER w_is_explicit AS rows_in_is_explicit,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        ROUND(CUME_DIST() OVER w_all_by_bpm, 3) AS bpm_cume_dist,
        ROUND(AVG(duration_sec_per_bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_duration_sec_per_bpm,
        MIN(duration_sec_per_bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec_per_bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_duration_sec_per_bpm_so_far,
        SUM(bpm) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        ROUND(AVG(duration_sec_per_bpm) OVER (ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_per_bpm_avg_1_prior,
        LEAD(bpm) OVER w_all_by_bpm_2 AS next_bpm
    FROM base_tracks
    WINDOW w_artist_id_by_duration_sec_pe AS (PARTITION BY artist_id ORDER BY duration_sec_per_bpm DESC, id), w_is_explicit AS (PARTITION BY is_explicit), w_all_by_bpm AS (ORDER BY bpm, id), w_all_by_bpm_2 AS (ORDER BY bpm DESC, id)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY rn ORDER BY rows_in_is_explicit, id) AS rows_in_is_explicit_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY rn, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY duration_sec_per_bpm_avg_1_prior, id), 3) AS duration_sec_per_bpm_avg_1_prior_percent_rank,
        NTH_VALUE(first_known_release_date, 2) OVER (PARTITION BY rn, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY duration_sec_per_bpm_avg_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_known_release_date,
        MAX(duration_sec_per_bpm) OVER (PARTITION BY rn, DATE_TRUNC('MONTH', last_known_release_date) ORDER BY duration_sec_per_bpm_avg_1_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_per_bpm_so_far
    FROM metrics
)
-- Final projection
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    moods,
    duration_sec_per_bpm,
    release_date_week,
    prev_duration_sec_per_bpm,
    last_known_release_date,
    next_release_date,
    rn,
    first_known_release_date,
    rows_in_is_explicit,
    artist_id_max_bpm,
    bpm_cume_dist,
    min_duration_sec_per_bpm_so_far,
    running_bpm,
    duration_sec_per_bpm_avg_1_prior,
    next_bpm,
    rows_in_is_explicit_tercile,
    duration_sec_per_bpm_avg_1_prior_percent_rank,
    second_first_known_release_date,
    max_duration_sec_per_bpm_so_far,
    rows_in_is_explicit - bpm AS rows_in_is_explicit_minus_bpm
FROM ranked
WHERE max_duration_sec_per_bpm_so_far > 35.0
ORDER BY second_first_known_release_date
