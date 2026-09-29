WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ABS(bpm - 94) AS bpm_distance
    FROM tracks
    WHERE artist_id <= 7
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        bpm_distance,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_3,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY duration_sec ASC), 3) AS duration_sec_cume_dist,
        DENSE_RANK() OVER (ORDER BY bpm_distance, id) AS bpm_distance_dense_rank
    FROM filtered
),
with_activity AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country = 'BR'
        )
)
SELECT
    bpm_distance_dense_rank,
    is_explicit,
    GROUPING(bpm_distance_dense_rank) AS is_total_bpm_distance_dense_rank,
    MIN(bpm_avg_3) AS min_bpm_avg_3,
    PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec,
    COUNT(*) AS cnt
FROM with_activity
GROUP BY ROLLUP(bpm_distance_dense_rank, is_explicit)
ORDER BY bpm_distance_dense_rank NULLS LAST, is_explicit NULLS LAST
