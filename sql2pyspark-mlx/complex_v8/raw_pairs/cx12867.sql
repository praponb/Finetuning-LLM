WITH filtered_tracks AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'C%'
      AND release_date >= DATE '2024-09-26'
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT s.*, a.genre AS artist_genre
    FROM filtered_tracks s
    JOIN artists a ON a.id = s.artist_id
),
flattened AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.artist_genre,
        t.mood_pos,
        t.mood
    FROM joined_artist s
    LATERAL VIEW POSEXPLODE(s.moods) t AS mood_pos, mood
)
-- Final projection
SELECT
    id,
    title,
    release_date,
    is_explicit,
    mood_pos,
    mood,
    CASE WHEN mood_pos >= 2 THEN 'large' WHEN mood_pos >= 1 THEN 'medium' ELSE 'small' END AS mood_pos_tier
FROM flattened
ORDER BY mood_pos_tier DESC, mood_pos NULLS LAST
