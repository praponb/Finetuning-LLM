WITH prepared AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        moods
    FROM tracks
    WHERE bpm <= 143
),
joined_artist AS (
    -- Attach artist attributes
    SELECT s.*, a.genre AS artist_genre
    FROM prepared s
    JOIN artists a ON a.id = s.artist_id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        s.artist_genre,
        p.id AS play_id,
        p.source AS play_source,
        p.device AS play_device
    FROM joined_artist s
    JOIN plays p ON p.track_id = s.id
)
-- Subtotals with CUBE
SELECT
    COALESCE(play_source, 'ALL') AS play_source,
    artist_id,
    COALESCE(artist_genre, 'ALL') AS artist_genre,
    GROUPING_ID() AS grouping_level,
    ARRAY_SORT(COLLECT_SET(play_device)) AS play_device_list
FROM exploded_detail
GROUP BY CUBE(play_source, artist_id, artist_genre)
ORDER BY play_source NULLS LAST, artist_id NULLS LAST, artist_genre NULLS LAST
