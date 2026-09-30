WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods
    FROM tracks
    WHERE LOWER(title) LIKE '%ld%'
),
with_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        a.genre AS artist_genre
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
grouped AS (
    -- Group-level totals
    SELECT
        artist_id,
        COUNT(*) FILTER (WHERE artist_genre = 'Jazz') AS n_artist_genre_jazz,
        SUM(CASE WHEN NOT is_explicit THEN 1 ELSE 0 END) AS n_not_is_explicit,
        COUNT(*) AS row_count,
        COUNT(DISTINCT artist_genre) AS distinct_artist_genres
    FROM with_details
    GROUP BY artist_id
)
SELECT
    artist_id,
    n_artist_genre_jazz,
    n_not_is_explicit,
    distinct_artist_genres,
    CASE WHEN distinct_artist_genres >= 6 THEN 'HIGH' WHEN distinct_artist_genres >= 5 THEN 'MEDIUM' ELSE 'LOW' END AS distinct_artist_genres_tier
FROM grouped
WHERE distinct_artist_genres BETWEEN 1 AND 8
ORDER BY distinct_artist_genres_tier NULLS LAST, artist_id DESC
