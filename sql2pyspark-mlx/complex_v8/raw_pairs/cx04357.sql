WITH valid_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        LOWER(REGEXP_REPLACE(TRIM(genre), ' ', '_')) AS genre_slug
    FROM artists
    WHERE country = 'KR'
      AND genre IN ('Indie', 'Pop', 'HipHop')
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT
        artist_id,
        ROUND(AVG(bpm), 2) AS avg_track_bpm,
        MIN(release_date) AS first_track_release_date,
        COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        t.avg_track_bpm,
        t.first_track_release_date,
        COALESCE(t.track_count, 0) AS track_count
    FROM valid_artists s
    LEFT JOIN track_stats t ON t.artist_id = s.id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count_2
    FROM profiled s
)
SELECT
    id,
    artist_name,
    genre,
    country,
    genre_slug,
    avg_track_bpm,
    first_track_release_date,
    track_count_2,
    COALESCE(avg_track_bpm, 0.0) AS avg_track_bpm_filled,
    LOWER(REGEXP_REPLACE(TRIM(genre_slug), ' ', '_')) AS genre_slug_slug
FROM with_stats
ORDER BY first_track_release_date, country, id
LIMIT 5
