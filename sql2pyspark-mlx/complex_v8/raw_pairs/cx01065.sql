WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        bpm,
        moods
    FROM tracks
    WHERE NOT ARRAY_CONTAINS(moods, 'focus')
      AND bpm >= 89
),
enriched AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.moods,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified
    FROM cleaned_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at_2
    FROM enriched s
),
exploded AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        s.artist_debut_year,
        s.artist_is_verified,
        s.last_played_at,
        s.last_played_at_2,
        t.mood
    FROM summarized_rows s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
with_details AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.bpm,
        s.artist_debut_year,
        s.artist_is_verified,
        s.last_played_at_2,
        s.mood,
        a.genre AS artist_genre,
        a.country AS artist_country
    FROM exploded s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    mood,
    artist_genre,
    artist_country
FROM with_details
ORDER BY artist_country NULLS LAST, artist_genre DESC NULLS FIRST
