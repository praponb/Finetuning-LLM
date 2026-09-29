WITH valid_artists AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_name,
        genre,
        is_verified
    FROM artists
    WHERE LENGTH(TRIM(artist_name)) >= 4
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        artist_name,
        genre,
        CASE WHEN genre IN ('Classical', 'HipHop') THEN 'group_a' WHEN genre = 'Pop' THEN 'group_b' ELSE 'other' END AS genre_group
    FROM valid_artists
    WHERE is_verified = TRUE
),
track_stats AS (
    -- Per-artist tracks metrics
    SELECT artist_id, COUNT(*) AS track_count
    FROM tracks
    GROUP BY artist_id
),
with_stats AS (
    -- Attach tracks metrics
    SELECT
        s.*,
        COALESCE(t.track_count, 0) AS track_count
    FROM annotated s
    LEFT JOIN track_stats t ON t.artist_id = s.id
)
SELECT id, genre, genre_group
FROM with_stats
ORDER BY id, genre
