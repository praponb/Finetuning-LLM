WITH base_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        bpm,
        moods,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter,
        SUBSTRING(TRIM(title), 1, 4) AS title_prefix,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix_2
    FROM tracks
    WHERE release_date IS NOT NULL
      AND duration_sec >= 184
),
per_label AS (
    -- One row per mood
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.bpm,
        s.release_date_quarter,
        s.title_prefix,
        s.title_prefix_2,
        t.mood
    FROM base_tracks s
    LATERAL VIEW EXPLODE(s.moods) t AS mood
),
joined AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre
    FROM per_label s
    JOIN artists a ON a.id = s.artist_id
    WHERE a.genre IN ('Jazz', 'Pop')
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    bpm,
    release_date_quarter,
    title_prefix,
    title_prefix_2,
    artist_genre,
    SUBSTRING(TRIM(artist_genre), 1, 2) AS artist_genre_prefix
FROM joined
WHERE LOWER(title) LIKE '%in%'
ORDER BY title_prefix_2 DESC
