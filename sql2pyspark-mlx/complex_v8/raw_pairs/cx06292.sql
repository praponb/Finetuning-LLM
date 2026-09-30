WITH recent_artists AS (
    -- Scope the artists rows
    SELECT
        id,
        artist_name,
        genre,
        country,
        CONCAT(genre, '#', CAST(id AS STRING)) AS genre_ref
    FROM artists
    WHERE (artist_name IS NOT NULL OR genre = 'HipHop')
),
detail_rows AS (
    -- One row per track
    SELECT
        s.id,
        s.artist_name,
        s.country,
        t.id AS track_id,
        t.duration_sec AS track_duration_sec,
        t.title AS track_title,
        t.release_date AS track_release_date,
        t.moods AS track_moods
    FROM recent_artists s
    LEFT JOIN tracks t ON t.artist_id = s.id
),
exploded AS (
    -- One row per track_mood
    SELECT
        s.id,
        s.artist_name,
        s.country,
        s.track_id,
        s.track_duration_sec,
        s.track_title,
        s.track_release_date,
        t.track_mood
    FROM detail_rows s
    LATERAL VIEW OUTER EXPLODE(s.track_moods) t AS track_mood
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM exploded
    WHERE track_duration_sec < (SELECT MAX(track_duration_sec) FROM exploded)
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.device = 'desktop' THEN 1 END) FROM plays p WHERE p.track_id = s.track_id) AS play_device_count
    FROM screened s
)
SELECT
    id,
    artist_name,
    track_id,
    track_duration_sec,
    track_title,
    track_release_date,
    track_mood,
    play_device_count,
    CONCAT_WS(' / ', track_mood, track_title) AS track_mood_track_title_label
FROM with_stats
ORDER BY play_device_count
