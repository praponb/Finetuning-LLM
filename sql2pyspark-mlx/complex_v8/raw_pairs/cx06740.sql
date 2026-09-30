WITH prepared AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE title IS NOT NULL
),
filtered AS (
    -- Rows with no matching tracks
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM tracks t
            WHERE t.artist_id = s.artist_id
              AND t.bpm >= 96
              AND t.is_explicit = TRUE
        )
),
filtered_2 AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.ms_played <= 246779
              AND p.listener_id IS NOT NULL
        )
),
enriched_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified,
        a.debut_year AS artist_debut_year
    FROM filtered_2 s
    LEFT JOIN artists a ON a.id = s.artist_id
)
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    release_date,
    bpm,
    moods,
    artist_genre,
    artist_is_verified,
    artist_debut_year,
    IF(artist_is_verified, 'yes', 'no') AS artist_is_verified_label,
    SUBSTRING(TRIM(title), 1, 4) AS title_prefix
FROM enriched_artist
ORDER BY title DESC
