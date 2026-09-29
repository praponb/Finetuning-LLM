WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods,
        a.genre AS artist_genre
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre = 'HipHop'
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        bpm,
        moods,
        artist_genre,
        LENGTH(TRIM(artist_genre)) AS artist_genre_len
    FROM joined_artist
    WHERE duration_sec <= 365
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT artist_genre_len, artist_genre, bpm
    FROM annotated
) src
PIVOT (
    SUM(bpm)
    FOR artist_genre IN ('HipHop', 'Pop', 'Classical', 'Indie')
)
ORDER BY artist_genre_len
