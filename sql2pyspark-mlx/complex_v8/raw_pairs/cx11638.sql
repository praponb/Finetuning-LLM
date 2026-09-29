WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_ADD(release_date, 7) AS release_date_plus_7d
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'N%'
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.device NOT IN ('speaker', 'car')
        )
      AND s.bpm >= 94
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year
    FROM with_activity s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Subtotals with GROUPING SETS
SELECT
    is_explicit,
    artist_id,
    COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false,
    MIN(bpm) AS min_bpm
FROM with_details
GROUP BY GROUPING SETS ((is_explicit, artist_id), (is_explicit), ())
ORDER BY is_explicit NULLS LAST, artist_id NULLS LAST
