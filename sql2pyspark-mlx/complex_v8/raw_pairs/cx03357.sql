WITH cleaned_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ARRAY_CONTAINS(moods, 'focus') AS has_focus,
        ROUND(duration_sec / NULLIF(bpm, 0), 3) AS duration_sec_per_bpm
    FROM tracks
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM cleaned_tracks s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.played_at >= TIMESTAMP '2025-02-06 09:00:00'
        )
      AND s.artist_id IS NOT NULL
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.moods,
        s.has_focus,
        s.duration_sec_per_bpm,
        a.artist_name,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM with_activity s
    LEFT JOIN artists a ON a.id = s.artist_id
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.has_focus,
        s.duration_sec_per_bpm,
        s.artist_name,
        s.artist_genre,
        s.artist_debut_year,
        t.mood
    FROM enriched s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
)
-- Final projection
SELECT
    id,
    artist_id,
    duration_sec,
    has_focus,
    artist_name,
    artist_genre,
    artist_debut_year,
    mood,
    LENGTH(TRIM(artist_name)) AS artist_name_len
FROM per_label
