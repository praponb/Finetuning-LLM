WITH filtered_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        LENGTH(TRIM(title)) AS title_len
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'M%'
),
joined_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.title_len,
        a.genre AS artist_genre,
        a.debut_year AS artist_debut_year
    FROM filtered_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(artist_genre, 'UNKNOWN') AS artist_genre_or_unknown,
        DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label,
        CONCAT(title, '#', CAST(title_len AS STRING)) AS title_ref
    FROM joined_artist
    WHERE artist_id <= 14
)
-- Final projection
SELECT
    id,
    title,
    duration_sec,
    release_date,
    is_explicit,
    bpm,
    title_len,
    artist_debut_year,
    artist_genre_or_unknown,
    release_date_label,
    title_ref,
    DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label_2
FROM derived
ORDER BY title_ref, artist_genre_or_unknown DESC, id
LIMIT 20
