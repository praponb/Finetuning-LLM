WITH recent_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        ADD_MONTHS(release_date, 1) AS release_date_plus_1m,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix
    FROM tracks
    WHERE SIZE(moods) >= 2
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        release_date_plus_1m,
        title_prefix,
        ROUND(PERCENT_RANK() OVER (ORDER BY duration_sec ASC), 3) AS duration_sec_percent_rank,
        MAX(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_max_6_prior,
        ROUND(bpm - AVG(bpm) OVER w_artist_id, 2) AS bpm_vs_artist_id_avg,
        MAX(bpm) OVER (PARTITION BY artist_id) AS artist_id_max_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        MAX(duration_sec) OVER (PARTITION BY artist_id ORDER BY release_date_plus_1m DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far,
        LEAD(bpm) OVER w_artist_id_by_release_date_pl_2 AS next_bpm,
        ROUND(AVG(bpm) OVER (PARTITION BY YEAR(release_date_plus_1m), artist_id ORDER BY bpm DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_1_prior,
        COUNT(*) OVER (PARTITION BY YEAR(release_date_plus_1m), artist_id ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM recent_tracks
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_id_by_release_date_pl_2 AS (PARTITION BY artist_id ORDER BY release_date_plus_1m DESC, id)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(title_prefix), ' ', '_')) AS title_prefix_slug
    FROM with_trends
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        release_date_plus_1m,
        duration_sec_percent_rank,
        bpm_max_6_prior,
        bpm_vs_artist_id_avg,
        bpm_share_of_artist_id,
        max_duration_sec_so_far,
        next_bpm,
        bpm_avg_1_prior,
        title_prefix_slug,
        LAST_VALUE(bpm_vs_artist_id_avg) OVER (PARTITION BY artist_id ORDER BY max_duration_sec_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_vs_artist_id_avg,
        LAG(running_count) OVER (PARTITION BY artist_id ORDER BY bpm_share_of_artist_id ASC, id) AS prev_running_count,
        SUM(bpm_avg_1_prior) OVER (PARTITION BY artist_id ORDER BY max_duration_sec_so_far, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_avg_1_prior,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY YEAR(release_date_plus_1m)), 0), 4) AS bpm_share_of_release_date_plus_1m_year,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY max_duration_sec_so_far) AS max_duration_sec_so_far_dense_rank,
        ROUND(max_duration_sec_so_far - AVG(max_duration_sec_so_far) OVER (PARTITION BY YEAR(release_date_plus_1m)), 2) AS max_duration_sec_so_far_vs_release_date_plus_1m_
    FROM derived
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    bpm,
    release_date_plus_1m,
    bpm_max_6_prior,
    bpm_share_of_artist_id,
    max_duration_sec_so_far,
    next_bpm,
    bpm_avg_1_prior,
    title_prefix_slug,
    last_bpm_vs_artist_id_avg,
    prev_running_count,
    running_bpm_avg_1_prior,
    max_duration_sec_so_far_dense_rank,
    ABS(max_duration_sec_so_far - 2145) AS max_duration_sec_so_far_distance,
    CONCAT(title_prefix_slug, '#', CAST(prev_running_count AS STRING)) AS title_prefix_slug_ref
FROM windowed
ORDER BY max_duration_sec_so_far_dense_rank, bpm_max_6_prior
