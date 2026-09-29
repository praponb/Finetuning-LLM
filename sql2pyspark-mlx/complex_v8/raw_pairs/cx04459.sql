WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        LENGTH(TRIM(title)) AS title_len,
        CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag,
        DATE_TRUNC('QUARTER', release_date) AS release_date_quarter
    FROM tracks
    WHERE (artist_id IS NOT NULL OR title IS NOT NULL)
),
enriched AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.bpm,
        s.title_len,
        s.is_explicit_flag,
        s.release_date_quarter,
        a.country AS artist_country
    FROM base s
    JOIN artists a ON a.id = s.artist_id
),
with_details_artist AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre
    FROM enriched s
    LEFT JOIN artists a ON a.id = s.artist_id
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    duration_sec,
    bpm,
    title_len,
    is_explicit_flag,
    release_date_quarter,
    artist_country,
    ABS(title_len - 6) AS title_len_distance,
    CONCAT_WS('|', artist_genre, title) AS artist_genre_title_label
FROM with_details_artist
ORDER BY title_len_distance DESC NULLS FIRST, id
LIMIT 5
