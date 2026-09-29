WITH scoped_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout
    FROM tracks
    WHERE bpm BETWEEN 94 AND 166
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY artist_id ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY duration_sec ASC), 3) AS duration_sec_cume_dist
    FROM scoped_tracks
),
derived AS (
    -- Derive helper columns
    SELECT *, LOWER(TRIM(title)) AS title_clean, ABS(bpm - 147) AS bpm_distance
    FROM windowed
    WHERE NOT is_explicit
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY has_workout, is_explicit ORDER BY bpm_distance ASC, id), 3) AS bpm_distance_cume_dist,
        LAST_VALUE(bpm_distance) OVER (PARTITION BY has_workout, is_explicit ORDER BY bpm_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm_distance,
        ROUND(bpm_distance - AVG(bpm_distance) OVER (PARTITION BY is_explicit, has_workout), 2) AS bpm_distance_vs_is_explicit_has_workout_avg,
        bpm_distance - LAG(bpm_distance) OVER w_has_workout_is_explicit_by_b AS bpm_distance_change
    FROM derived
    WINDOW w_has_workout_is_explicit_by_b AS (PARTITION BY has_workout, is_explicit ORDER BY bpm_distance ASC, id)
)
-- Subtotals with CUBE
SELECT
    has_workout,
    artist_id,
    ROUND(SUM(bpm_distance_vs_is_explicit_has_workout_avg) / NULLIF(SUM(bpm_distance_change), 0), 3) AS bpm_distance_vs_is_explicit_has_workout_avg_to_b,
    COUNT(DISTINCT id) AS distinct_ids
FROM with_trends
GROUP BY CUBE(has_workout, artist_id)
ORDER BY has_workout NULLS LAST, artist_id NULLS LAST
