WITH prepared_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        moods,
        DATE_TRUNC('YEAR', release_date) AS release_date_year,
        DATE_ADD(release_date, 30) AS release_date_plus_30d
    FROM tracks
),
enriched AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.moods,
        s.release_date_plus_30d,
        a.country AS artist_country,
        a.artist_name,
        a.genre AS artist_genre
    FROM prepared_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date_plus_30d,
        s.artist_country,
        s.artist_name,
        s.artist_genre,
        t.mood_pos,
        t.mood
    FROM enriched s
    LATERAL VIEW POSEXPLODE(s.moods) t AS mood_pos, mood
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date_plus_30d,
    artist_country,
    artist_name,
    artist_genre,
    mood
FROM exploded
ORDER BY title DESC NULLS FIRST
