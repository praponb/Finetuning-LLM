WITH base_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        genre,
        country,
        is_verified,
        LOWER(TRIM(country)) AS country_clean,
        LOWER(TRIM(artist_name)) AS artist_name_clean,
        COALESCE(genre, 'UNKNOWN') AS genre_or_unknown
    FROM artists
    WHERE debut_year >= 1994
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, MIN(release_date) AS first_track_release_date
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.first_track_release_date
    FROM base_artists s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
with_counts AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT SUM(t.bpm) FROM tracks t WHERE t.artist_id = s.id) AS total_track_bpm,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count,
        (SELECT MAX(t.release_date) FROM tracks t WHERE t.artist_id = s.id) AS last_track_release_date
    FROM profiled s
)
SELECT
    id,
    is_verified,
    artist_name_clean,
    genre_or_unknown,
    first_track_release_date,
    total_track_bpm,
    track_count,
    COALESCE(total_track_bpm, 0) AS total_track_bpm_filled,
    CONCAT(genre, '#', CAST(id AS STRING)) AS genre_ref
FROM with_counts
ORDER BY artist_name_clean, id DESC
