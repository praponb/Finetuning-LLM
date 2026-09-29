WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        ARRAY_CONTAINS(moods, 'workout') AS has_workout
    FROM tracks
    WHERE artist_id IS NOT NULL
      AND YEAR(release_date) = 2024
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LEAD(bpm) OVER w_artist_id_is_explicit_by_bpm AS next_bpm,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id), 0), 4) AS bpm_share_of_artist_id,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit, has_workout), 2) AS bpm_vs_is_explicit_has_workout_avg,
        MIN(bpm) OVER (PARTITION BY artist_id) AS artist_id_min_bpm,
        RANK() OVER (PARTITION BY has_workout ORDER BY bpm, id) AS bpm_rank,
        LAST_VALUE(bpm) OVER (ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id
    FROM scoped_tracks
    WINDOW w_artist_id_is_explicit_by_bpm AS (PARTITION BY artist_id, is_explicit ORDER BY bpm, id), w_artist_id AS (PARTITION BY artist_id)
),
leaders AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE bpm_rank = 1
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        bpm_rank,
        artist_id,
        COUNT(*) AS row_count,
        ROUND(AVG(bpm_vs_is_explicit_has_workout_avg), 2) AS avg_bpm_vs_is_explicit_has_workout_avg
    FROM leaders
    GROUP BY bpm_rank, artist_id
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    bpm_rank,
    artist_id,
    row_count,
    avg_bpm_vs_is_explicit_has_workout_avg,
    ABS(bpm_rank - 6) AS bpm_rank_distance
FROM rollup_base
ORDER BY artist_id DESC NULLS FIRST, bpm_rank
LIMIT 20
