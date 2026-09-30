WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        moods,
        ADD_MONTHS(release_date, 12) AS release_date_plus_12m,
        duration_sec + bpm AS duration_sec_plus_bpm
    FROM tracks
),
with_activity AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
              AND p.listener_id <= 6
        )
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, a.genre AS artist_genre, a.country AS artist_country
    FROM with_activity s
    JOIN artists a ON a.id = s.artist_id
)
SELECT
    COALESCE(artist_genre, 'ALL') AS artist_genre,
    COALESCE(artist_country, 'ALL') AS artist_country,
    artist_id,
    COUNT(DISTINCT artist_id) AS distinct_artist_ids
FROM joined
GROUP BY GROUPING SETS ((artist_genre, artist_country, artist_id), (artist_genre), (artist_country))
