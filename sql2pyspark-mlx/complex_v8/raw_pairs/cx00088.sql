WITH base_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        is_explicit,
        bpm,
        ARRAY_CONTAINS(moods, 'focus') AS has_focus,
        CASE WHEN duration_sec >= 309 THEN 'A' WHEN duration_sec >= 154 THEN 'B' ELSE 'C' END AS duration_sec_tier,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label
    FROM tracks
    WHERE artist_id IS NOT NULL
),
combined_details AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre
    FROM base_tracks s
    JOIN artists a ON a.id = s.artist_id
),
joined AS (
    -- Attach artist attributes
    SELECT s.*, a.country AS artist_country, a.artist_name
    FROM combined_details s
    JOIN artists a ON a.id = s.artist_id
    WHERE LOWER(a.artist_name) LIKE '%it%'
)
SELECT *
FROM (
    SELECT is_explicit_label, artist_genre, duration_sec
    FROM joined
) src
PIVOT (
    SUM(duration_sec)
    FOR artist_genre IN ('Indie', 'HipHop')
)
ORDER BY is_explicit_label
