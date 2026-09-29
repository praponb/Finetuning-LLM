WITH valid AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods
    FROM tracks
    WHERE is_explicit = TRUE
),
joined_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.bpm,
        s.moods,
        a.is_verified AS artist_is_verified
    FROM valid s
    JOIN artists a ON a.id = s.artist_id
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.moods,
        s.artist_is_verified,
        a.artist_name,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified_2
    FROM joined_artist s
    LEFT JOIN artists a ON a.id = s.artist_id
),
line_level AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.moods,
        s.artist_genre,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.device AS play_device
    FROM with_parent s
    JOIN plays p ON p.track_id = s.id AND p.device <> 'desktop'
)
SELECT
    id,
    artist_id,
    release_date,
    artist_genre,
    play_id,
    play_is_skipped,
    play_source,
    play_device,
    LENGTH(TRIM(play_device)) AS play_device_len,
    SIZE(moods) AS moods_count
FROM line_level
ORDER BY play_device_len DESC NULLS FIRST, artist_id DESC, id, play_id
LIMIT 10
