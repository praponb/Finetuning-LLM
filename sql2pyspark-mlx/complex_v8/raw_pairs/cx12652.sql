WITH filtered AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref,
        GREATEST(duration_sec, bpm) AS greatest_duration_sec_bpm
    FROM tracks
    WHERE ARRAY_CONTAINS(moods, 'sad')
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MAX(p.played_at) FROM plays p WHERE p.track_id = s.id) AS last_played_at
    FROM filtered s
),
joined_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.is_explicit,
        s.moods,
        s.title_ref,
        s.greatest_duration_sec_bpm,
        a.country AS artist_country,
        a.genre AS artist_genre
    FROM with_counts s
    LEFT JOIN artists a ON a.id = s.artist_id
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.is_explicit,
        s.artist_genre,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.listener_id AS play_listener_id
    FROM joined_artist s
    JOIN plays p ON p.track_id = s.id
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    artist_genre,
    play_id,
    play_played_at,
    play_listener_id,
    play_played_at + INTERVAL 15 MINUTES AS play_played_at_plus_15m
FROM line_level
ORDER BY artist_genre DESC, play_played_at_plus_15m DESC NULLS FIRST
