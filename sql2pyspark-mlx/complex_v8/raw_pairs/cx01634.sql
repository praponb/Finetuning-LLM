WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
    WHERE duration_sec <= 402
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        duration_sec_bucket,
        duration_sec_per_bpm,
        LEAD(bpm, 2) OVER (PARTITION BY is_explicit ORDER BY bpm, id) AS next2_bpm,
        bpm - LAG(bpm) OVER w_artist_id_by_release_date AS bpm_change,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY bpm), 3) AS bpm_cume_dist,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS bpm_vs_artist_id_is_explicit_avg,
        COUNT(*) OVER w_artist_id_is_explicit AS rows_in_artist_id_is_explicit,
        MIN(bpm) OVER (ORDER BY duration_sec_per_bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        ROUND(PERCENT_RANK() OVER w_all_by_duration_sec_per_bpm_2, 3) AS duration_sec_per_bpm_percent_rank,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY is_explicit)) AS distinct_artist_id_in_is_explicit,
        ROUND(duration_sec_per_bpm - AVG(duration_sec_per_bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS duration_sec_per_bpm_vs_artist_id_is_explicit_av,
        DENSE_RANK() OVER (ORDER BY duration_sec_per_bpm DESC NULLS FIRST, id) AS duration_sec_per_bpm_dense_rank,
        ROUND(duration_sec_bucket - AVG(duration_sec_bucket) OVER (PARTITION BY is_explicit), 2) AS duration_sec_bucket_vs_is_explicit_avg
    FROM cleaned
    WINDOW w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_all_by_duration_sec_per_bpm_2 AS (ORDER BY duration_sec_per_bpm DESC NULLS FIRST)
)
SELECT
    id,
    is_explicit,
    moods,
    duration_sec_bucket,
    duration_sec_per_bpm,
    next2_bpm,
    bpm_change,
    bpm_cume_dist,
    min_bpm_so_far,
    duration_sec_per_bpm_percent_rank,
    distinct_artist_id_in_is_explicit,
    duration_sec_per_bpm_dense_rank,
    duration_sec_bucket_vs_is_explicit_avg,
    LEAST(bpm_cume_dist, duration_sec_bucket) AS least_bpm_cume_dist_duration_sec_bucket
FROM metrics
ORDER BY duration_sec_per_bpm DESC NULLS FIRST, id
LIMIT 5
