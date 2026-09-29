WITH prepared_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified
    FROM artists
    WHERE NOT is_verified
      AND artist_name IS NOT NULL
),
profiled AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count,
        (SELECT COUNT(*) FROM tracks t WHERE t.artist_id = s.id) AS track_count_2
    FROM prepared_artists s
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM profiled
    WHERE track_count > (SELECT AVG(track_count) FROM profiled)
)
SELECT
    id,
    artist_name,
    genre,
    country,
    debut_year,
    is_verified,
    track_count
FROM with_activity
ORDER BY genre, track_count
