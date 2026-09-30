WITH filtered_tracks AS (
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
        ROUND(bpm / NULLIF(duration_sec, 0), 3) AS bpm_per_duration_sec
    FROM tracks
    WHERE artist_id >= 2
)
SELECT title, release_date AS day, 'track' AS source
FROM filtered_tracks
UNION ALL
SELECT title, release_date AS day, 'track_2' AS source
FROM tracks
WHERE is_explicit = FALSE
