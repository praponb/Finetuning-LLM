WITH valid_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec - bpm AS duration_sec_minus_bpm,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE artist_id IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        title,
        is_explicit,
        duration_sec_minus_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm NULLS LAST), 3) AS duration_sec_minus_bpm_percent_rank,
        LEAD(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm, id) AS next_bpm,
        ROUND(AVG(duration_sec_minus_bpm) OVER (PARTITION BY is_explicit), 2) AS is_explicit_avg_duration_sec_minus_bpm,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm, id), 3) AS duration_sec_minus_bpm_cume_dist,
        LAG(release_date) OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm, id) AS prev_release_date,
        SUM(duration_sec_minus_bpm) OVER w_is_explicit_release_date_mon AS is_explicit_release_date_month_sum_duration_sec_,
        SUM(duration_sec_minus_bpm) OVER (PARTITION BY is_explicit) AS is_explicit_sum_duration_sec_minus_bpm,
        SUM(bpm) OVER (ORDER BY release_date ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        DENSE_RANK() OVER w_artist_id_is_explicit_by_bpm_2 AS bpm_dense_rank,
        DENSE_RANK() OVER (ORDER BY release_date ASC NULLS LAST) AS release_date_dense_rank,
        LAST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(AVG(duration_sec_minus_bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS duration_sec_minus_bpm_avg_6_prior
    FROM valid_tracks
    WINDOW w_is_explicit_release_date_mon AS (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date)), w_artist_id_is_explicit_by_bpm_2 AS (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(first_known_bpm / 25) * 25 AS first_known_bpm_bucket,
        SUBSTRING(TRIM(title), 1, 3) AS title_prefix
    FROM windowed
    WHERE running_bpm <= 1191
)
SELECT
    id,
    title,
    is_explicit,
    duration_sec_minus_bpm,
    duration_sec_minus_bpm_percent_rank,
    is_explicit_avg_duration_sec_minus_bpm,
    first_known_bpm,
    duration_sec_minus_bpm_cume_dist,
    prev_release_date,
    is_explicit_sum_duration_sec_minus_bpm,
    running_bpm,
    bpm_dense_rank,
    release_date_dense_rank,
    last_known_release_date,
    duration_sec_minus_bpm_avg_6_prior,
    first_known_bpm_bucket,
    COALESCE(duration_sec_minus_bpm, 0) AS duration_sec_minus_bpm_filled,
    CASE WHEN bpm_dense_rank >= 8 THEN 'A' WHEN bpm_dense_rank >= 4 THEN 'B' ELSE 'C' END AS bpm_dense_rank_tier
FROM enriched
WHERE prev_release_date < DATE '2025-03-29'
ORDER BY first_known_bpm, title
