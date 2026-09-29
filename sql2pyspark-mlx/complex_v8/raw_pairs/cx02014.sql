WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM tracks
    WHERE LOWER(title) LIKE '%on%'
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM cleaned_tracks s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.id
        )
),
enriched_artist AS (
    -- Join artists
    SELECT s.*, a.genre AS artist_genre
    FROM without_matches s
    JOIN artists a ON a.id = s.artist_id
)
SELECT id, metric_name, value
FROM (
    SELECT id, duration_sec_filled, bpm
    FROM enriched_artist
) src
UNPIVOT (
    value FOR metric_name IN (duration_sec_filled, bpm)
) unp
