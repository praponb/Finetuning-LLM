WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        is_explicit,
        bpm
    FROM tracks
    WHERE is_explicit = TRUE
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.artist_name,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.debut_year <= 2018
),
joined AS (
    -- Attach artist attributes
    SELECT
        s.*,
        a.country AS artist_country,
        a.is_verified AS artist_is_verified,
        a.genre AS artist_genre_2
    FROM enriched s
    JOIN artists a ON a.id = s.artist_id
),
with_parent AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.is_explicit,
        s.bpm,
        s.artist_name,
        s.artist_debut_year,
        s.artist_country,
        s.artist_is_verified,
        a.is_verified AS artist_is_verified_2
    FROM joined s
    JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    is_explicit,
    bpm,
    artist_name,
    artist_debut_year,
    artist_country,
    artist_is_verified_2,
    artist_debut_year + bpm AS artist_debut_year_plus_bpm
FROM with_parent
