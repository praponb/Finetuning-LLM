WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        bpm,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date
    FROM tracks
    WHERE artist_id <= 11
),
screened AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM filtered_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND UPPER(TRIM(t.title)) LIKE 'S%'
        )
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.days_since_release_date,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM screened s
    JOIN artists a ON a.id = s.artist_id AND a.country <> 'KR'
)
-- Subtotals with ROLLUP
SELECT
    artist_id,
    artist_is_verified,
    SUM(artist_debut_year) AS total_artist_debut_year,
    PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec,
    COUNT(*) AS row_count
FROM joined_artist
GROUP BY ROLLUP(artist_id, artist_is_verified)
ORDER BY artist_id NULLS LAST, artist_is_verified NULLS LAST
