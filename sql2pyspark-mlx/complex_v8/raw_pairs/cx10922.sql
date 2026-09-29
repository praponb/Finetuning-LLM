WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.artist_name
    FROM filtered s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre NOT IN ('Jazz', 'HipHop')
),
filtered_2 AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM with_parent s
    WHERE EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country IN ('KR', 'BR')
        )
)
-- Subtotals with ROLLUP
SELECT
    artist_genre,
    is_explicit,
    PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec
FROM filtered_2
GROUP BY ROLLUP(artist_genre, is_explicit)
ORDER BY artist_genre NULLS LAST, is_explicit NULLS LAST
