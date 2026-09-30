WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        bpm,
        moods,
        DATEDIFF(DATE '2025-06-30', release_date) AS days_since_release_date,
        QUARTER(release_date) AS release_date_quarter
    FROM tracks
),
with_parent_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.moods,
        s.days_since_release_date,
        a.artist_name,
        a.genre AS artist_genre
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
screened AS (
    -- Keep rows whose artist matches
    SELECT s.*
    FROM with_parent_artist s
    WHERE NOT EXISTS (
            SELECT 1
            FROM artists a
            WHERE a.id = s.artist_id
              AND a.country = 'KR'
              AND a.debut_year < 2015
        )
),
enriched AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods,
        artist_name,
        artist_genre,
        ABS(days_since_release_date - 210) AS days_since_release_date_distance
    FROM screened
)
-- Final projection
SELECT
    id,
    artist_id,
    moods,
    artist_name,
    artist_genre,
    days_since_release_date_distance,
    COALESCE(bpm, 0) AS bpm_filled
FROM enriched
