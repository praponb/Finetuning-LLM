WITH scoped_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        SIZE(moods) AS moods_count,
        DAYOFWEEK(release_date) AS release_date_dayofweek,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'S%'
      AND bpm <= 142
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.bpm,
        s.moods_count,
        s.title_len,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM scoped_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
without_matches AS (
    -- Rows that have matching tracks
    SELECT s.*
    FROM combined_details_artist s
    WHERE s.artist_id IN (
            SELECT t.artist_id
            FROM tracks t
            WHERE t.is_explicit = TRUE
        )
),
combined_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.moods_count,
        s.title_len,
        s.artist_genre,
        s.artist_debut_year,
        s.artist_is_verified,
        a.debut_year AS artist_debut_year_2,
        a.is_verified AS artist_is_verified_2,
        a.genre AS artist_genre_2
    FROM without_matches s
    JOIN artists a ON a.id = s.artist_id
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT moods_count, artist_genre_2, artist_genre, duration_sec
    FROM combined_details
) src
PIVOT (
    MIN(duration_sec)
    FOR artist_genre IN ('Indie', 'Pop', 'Classical', 'HipHop')
)
ORDER BY moods_count, artist_genre_2
