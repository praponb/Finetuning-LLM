WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        debut_year,
        is_verified
    FROM artists
    WHERE genre NOT IN ('HipHop', 'Indie')
),
with_children_tracks AS (
    -- One row per track
    SELECT
        s.id,
        s.genre,
        s.debut_year,
        t.id AS track_id,
        t.title AS track_title,
        t.release_date AS track_release_date
    FROM cleaned s
    JOIN tracks t ON t.artist_id = s.id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT track_id, track_release_date, genre, debut_year
    FROM with_children_tracks
) src
PIVOT (
    MIN(debut_year)
    FOR genre IN ('Pop', 'Indie', 'HipHop', 'Jazz')
)
ORDER BY track_id, track_release_date
